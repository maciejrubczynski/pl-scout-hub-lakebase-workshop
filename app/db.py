"""
Database Connection Module for PL Live Scout Hub
Manages connections to Lakebase OLTP/OLAP bridge via psycopg2
Uses Databricks SDK's database.generate_database_credential() for auth
"""

import psycopg2
import pandas as pd
from typing import Optional, Dict
import os
import logging
from datetime import datetime, timedelta
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Lakebase Connection Parameters
# When running as Databricks App with a database resource,
# PG* env vars are automatically set by the platform
LAKEBASE_HOST = os.getenv("PGHOST", os.getenv(
    "LAKEBASE_HOST",
    "instance-f2154f6b-faed-4b9f-992f-c3e46384cc3b.database.azuredatabricks.net"
))
LAKEBASE_PORT = int(os.getenv("PGPORT", os.getenv("LAKEBASE_PORT", "5432")))
LAKEBASE_DB = os.getenv("PGDATABASE", os.getenv("LAKEBASE_DB", "pl_scout_db"))
LAKEBASE_USER = os.getenv("PGUSER", os.getenv("LAKEBASE_USER", "rubczynski.maciej@gmail.com"))
LAKEBASE_SSLMODE = os.getenv("PGSSLMODE", "require")

# Lakebase instance name (for credential generation)
LAKEBASE_INSTANCE = os.getenv("LAKEBASE_INSTANCE", "pl-scout-hub")

# Connection timeout
CONNECT_TIMEOUT = 30

logger.info(f"Lakebase config: host={LAKEBASE_HOST}, port={LAKEBASE_PORT}, "
            f"db={LAKEBASE_DB}, user={LAKEBASE_USER}, ssl={LAKEBASE_SSLMODE}, "
            f"instance={LAKEBASE_INSTANCE}")

# ============================================================================
# GLOBAL STATE
# ============================================================================

_token_cache: Dict[str, any] = {
    "token": None,
    "expires_at": None,
}

_workspace_client = None


# ============================================================================
# TOKEN MANAGEMENT
# ============================================================================

def _get_workspace_client():
    """Get or create a WorkspaceClient singleton."""
    global _workspace_client
    if _workspace_client is None:
        from databricks.sdk import WorkspaceClient
        _workspace_client = WorkspaceClient()
        logger.info("WorkspaceClient initialized")
    return _workspace_client


def _get_lakebase_token() -> str:
    """
    Get a database credential token for Lakebase using the Databricks SDK.
    Uses database.generate_database_credential() (instances API).

    Returns:
        str: Valid database token for Lakebase PostgreSQL connection
    """
    # Check if cached token is still valid (refresh 5 min before expiry)
    if _token_cache["token"] and _token_cache["expires_at"]:
        if datetime.utcnow() < (_token_cache["expires_at"] - timedelta(minutes=5)):
            return _token_cache["token"]

    try:
        ws = _get_workspace_client()

        # Generate database credential using the instances API
        # This is the correct method for Lakebase Provisioned instances
        cred = ws.database.generate_database_credential(
            request_id=str(uuid.uuid4()),
            instance_names=[LAKEBASE_INSTANCE]
        )
        token = cred.token

        # Cache token with 50 min expiration (tokens last ~1 hour)
        _token_cache["token"] = token
        _token_cache["expires_at"] = datetime.utcnow() + timedelta(minutes=50)

        logger.info("Lakebase database credential obtained successfully")
        return token

    except Exception as e:
        logger.error(f"Failed to get Lakebase credential: {e}")
        logger.warning("Trying fallback auth methods...")
        return _get_oauth_token_fallback()


def _get_oauth_token_fallback() -> str:
    """Fallback: try alternative token methods."""
    # Try DATABRICKS_TOKEN env var
    token = os.getenv("DATABRICKS_TOKEN", "")
    if token:
        logger.info("Using DATABRICKS_TOKEN env var as fallback")
        return token

    raise ValueError(
        "Could not obtain authentication token for Lakebase. "
        "Ensure the app has a database resource configured and "
        "databricks-sdk>=0.61.0 is installed."
    )


# ============================================================================
# CONNECTION MANAGEMENT
# ============================================================================

def get_connection():
    """
    Get a fresh connection to Lakebase.
    Each connection uses a fresh or cached token.
    """
    token = _get_lakebase_token()

    try:
        conn = psycopg2.connect(
            host=LAKEBASE_HOST,
            port=LAKEBASE_PORT,
            database=LAKEBASE_DB,
            user=LAKEBASE_USER,
            password=token,
            sslmode=LAKEBASE_SSLMODE,
            connect_timeout=CONNECT_TIMEOUT,
            options="-c statement_timeout=30000",
        )
        logger.debug("Connection to Lakebase established")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Lakebase: {e}")
        raise


def return_connection(conn):
    """Close a connection."""
    if conn:
        try:
            conn.close()
        except Exception:
            pass


# ============================================================================
# QUERY EXECUTION
# ============================================================================

def execute_query(query: str, params: Optional[tuple] = None) -> pd.DataFrame:
    """Execute a SELECT query and return results as DataFrame."""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        df = pd.DataFrame(rows, columns=columns)
        logger.info(f"Query returned {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Query execution failed: {e}\nQuery: {query}")
        raise
    finally:
        return_connection(conn)


def execute_write(query: str, params: Optional[tuple] = None) -> Dict:
    """Execute an INSERT/UPDATE/DELETE query (OLTP write)."""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        rowcount = cursor.rowcount
        conn.commit()
        cursor.close()
        logger.info(f"Write query executed: {rowcount} rows affected")
        return {
            "success": True,
            "rows_affected": rowcount,
            "timestamp": datetime.utcnow().isoformat(),
        }
    except Exception as e:
        logger.error(f"Write execution failed: {e}\nQuery: {query}")
        if conn:
            conn.rollback()
        raise
    finally:
        return_connection(conn)


def execute_transaction(queries: list) -> Dict:
    """Execute multiple queries in a single transaction."""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        for query in queries:
            cursor.execute(query)
        conn.commit()
        cursor.close()
        logger.info(f"Transaction completed: {len(queries)} queries executed")
        return {
            "success": True,
            "queries_executed": len(queries),
            "timestamp": datetime.utcnow().isoformat(),
        }
    except Exception as e:
        logger.error(f"Transaction failed: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        return_connection(conn)


# ============================================================================
# HEALTH CHECK
# ============================================================================

def health_check() -> Dict:
    """Check database connection health."""
    try:
        result = execute_query("SELECT 1 as status")
        return {
            "status": "healthy",
            "connected": True,
            "timestamp": datetime.utcnow().isoformat(),
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "connected": False,
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat(),
        }


def get_table_info(table_name: str) -> Dict:
    """Get information about a table."""
    try:
        query = """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position
        """
        result = execute_query(query, (table_name,))
        return {
            "table_name": table_name,
            "columns": result.to_dict(orient="records"),
            "row_count": execute_query(f"SELECT COUNT(*) FROM {table_name}").iloc[0, 0],
        }
    except Exception as e:
        logger.error(f"Failed to get table info: {e}")
        return {"table_name": table_name, "error": str(e)}


# ============================================================================
# CLEANUP
# ============================================================================

def cleanup():
    """Clean up resources on shutdown."""
    logger.info("Database module cleanup complete")

import atexit
atexit.register(cleanup)
