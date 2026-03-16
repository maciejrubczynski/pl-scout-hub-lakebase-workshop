"""
PL Live Scout Hub - Data Generation Script
Generates realistic Premier League player and match data for the Lakebase workshop demo.
"""

import csv
import random
from datetime import datetime, timedelta
from typing import List, Dict, Tuple

# Real Premier League players (2024/25 season) across all 20 teams
PL_PLAYERS = {
    # Manchester City
    "Manchester City": [
        ("KC001", "Kevin De Bruyne", "MID", "Belgian", 33),
        ("JA001", "Jack Grealish", "MID", "English", 29),
        ("ER001", "Erling Haaland", "FWD", "Norwegian", 24),
        ("JO001", "John Stones", "DEF", "English", 30),
        ("MA001", "Manuel Akanji", "DEF", "German", 29),
        ("RO001", "Rodri", "MID", "Spanish", 28),
        ("PH001", "Phil Foden", "MID", "English", 24),
        ("ED001", "Ederson", "GK", "Brazilian", 31),
        ("KA001", "Kalvin Phillips", "MID", "English", 29),
        ("JU001", "Julian Álvarez", "FWD", "Argentine", 24),
    ],
    # Liverpool
    "Liverpool": [
        ("MO001", "Mohamed Salah", "FWD", "Egyptian", 32),
        ("VI001", "Virgil van Dijk", "DEF", "Dutch", 33),
        ("TR001", "Trent Alexander-Arnold", "DEF", "English", 26),
        ("AL001", "Alisson", "GK", "Brazilian", 32),
        ("LU001", "Luis Díaz", "FWD", "Colombian", 27),
        ("JO002", "Jota", "MID", "Portuguese", 27),
        ("AN001", "Andy Robertson", "DEF", "Scottish", 30),
        ("RY001", "Ryan Gravenberch", "MID", "Dutch", 22),
        ("DO001", "Dominic Szoboszlai", "MID", "Hungarian", 23),
        ("CU001", "Curtis Jones", "MID", "English", 23),
    ],
    # Arsenal
    "Arsenal": [
        ("BU001", "Bukayo Saka", "FWD", "English", 23),
        ("MO002", "Martin Ødegaard", "MID", "Norwegian", 25),
        ("GA001", "Gabriel Martinelli", "FWD", "Brazilian", 23),
        ("WH001", "William Saliba", "DEF", "French", 24),
        ("BE001", "Ben White", "DEF", "English", 27),
        ("AE001", "Aaron Ramsdale", "GK", "English", 26),
        ("TA001", "Thomas Partey", "MID", "Ghanaian", 31),
        ("JO003", "Jorginho", "MID", "Italian", 32),
        ("DE001", "Declan Rice", "MID", "English", 25),
        ("KA002", "Kai Havertz", "FWD", "German", 25),
    ],
    # Manchester United
    "Manchester United": [
        ("BR001", "Bruno Fernandes", "MID", "Portuguese", 30),
        ("CA001", "Casemiro", "MID", "Brazilian", 32),
        ("MA002", "Mason Mount", "MID", "English", 25),
        ("AN002", "Antony", "FWD", "Brazilian", 24),
        ("JA002", "Jadon Sancho", "FWD", "English", 24),
        ("DA001", "David de Gea", "GK", "Spanish", 34),
        ("AR001", "Armando Broja", "FWD", "Albanian", 22),
        ("VA001", "Wan-Bissaka", "DEF", "English", 26),
        ("HA001", "Harry Maguire", "DEF", "English", 31),
        ("LI001", "Lisandro Martínez", "DEF", "Argentine", 25),
    ],
    # Chelsea
    "Chelsea": [
        ("CO001", "Cole Palmer", "MID", "English", 22),
        ("NI001", "Nicolas Jackson", "FWD", "Senegalese", 23),
        ("MO003", "Moisés Caicedo", "MID", "Ecuadorian", 22),
        ("EN001", "Enzo Fernández", "MID", "Argentine", 23),
        ("MA003", "Mauricio Pochettino", "MID", "Argentine", 22),
        ("JS001", "Jamal Murray", "DEF", "English", 23),
        ("RE001", "Reece James", "DEF", "English", 24),
        ("BE002", "Benoit Badiashile", "DEF", "French", 24),
        ("RO002", "Robert Sánchez", "GK", "Spanish", 26),
        ("SU001", "Sunny Sunley", "DEF", "English", 21),
    ],
    # Tottenham
    "Tottenham": [
        ("SO001", "Son Heung-min", "FWD", "South Korean", 32),
        ("MA004", "Maddison", "MID", "English", 27),
        ("RI001", "Richarlison", "FWD", "Brazilian", 27),
        ("DA002", "Davinson Sánchez", "DEF", "Colombian", 28),
        ("CR001", "Cristian Romero", "DEF", "Argentine", 26),
        ("PI001", "Pierre-Emile Højbjerg", "MID", "Danish", 28),
        ("VI002", "Vicário", "GK", "Spanish", 27),
        ("PA001", "Pape Sarr", "MID", "Senegalese", 22),
        ("YO001", "Yves Bissouma", "MID", "Malian", 25),
        ("BR002", "Brennan Johnson", "FWD", "English", 23),
    ],
    # Bournemouth
    "Bournemouth": [
        ("AT001", "Enes Ünal", "FWD", "Turkish", 27),
        ("DO002", "Dominic Solanke", "FWD", "English", 27),
        ("MI001", "Milos Kerkez", "DEF", "Serbian", 21),
        ("LL001", "Lloyd Kelly", "DEF", "English", 26),
        ("MA005", "Mark Travers", "GK", "Irish", 23),
        ("AB001", "Antoine Semenyo", "FWD", "Ghanaian", 24),
        ("JA003", "Jamal Murray", "DEF", "English", 26),
        ("TA002", "Talisk Pronk", "MID", "Dutch", 26),
        ("EM001", "Emiliano Martínez", "GK", "Argentine", 32),
        ("DW001", "David Datro Fofana", "FWD", "Ivorian", 22),
    ],
    # Aston Villa
    "Aston Villa": [
        ("OL001", "Ollie Watkins", "FWD", "English", 28),
        ("JA004", "Jaden Smith", "DEF", "English", 23),
        ("JO004", "John McGinn", "MID", "Scottish", 29),
        ("PA002", "Pau Torres", "DEF", "Spanish", 27),
        ("EM002", "Emiliano Martínez", "GK", "Argentine", 32),
        ("MO004", "Morgan Rogers", "MID", "English", 22),
        ("DI001", "Diego Carlos", "DEF", "Brazilian", 30),
        ("KO001", "Konsa", "DEF", "English", 26),
        ("RO003", "Rob Holding", "DEF", "English", 29),
        ("LE001", "Leon Bailey", "FWD", "Jamaican", 27),
    ],
    # Nottingham Forest
    "Nottingham Forest": [
        ("CR002", "Chris Wood", "FWD", "New Zealand", 33),
        ("WA001", "Wander", "DEF", "Brazilian", 25),
        ("MU001", "Murillo", "DEF", "Brazilian", 22),
        ("AW001", "Awoniyi", "FWD", "Nigerian", 26),
        ("MA006", "Mats Konoplyanka", "MID", "Ukrainian", 34),
        ("BR003", "Brennan Johnson", "FWD", "English", 23),
        ("SC001", "Scott McKenna", "DEF", "Scottish", 27),
        ("NE001", "Neco Williams", "DEF", "Welsh", 23),
        ("MA007", "Matt Turner", "GK", "American", 29),
        ("JO005", "Jonjo Shelvey", "MID", "English", 32),
    ],
    # Brighton
    "Brighton": [
        ("JA005", "João Pedro", "FWD", "Portuguese", 21),
        ("AI001", "Alexis Mac Allister", "MID", "Argentine", 25),
        ("MO005", "Moisés Caicedo", "MID", "Ecuadorian", 22),
        ("WE001", "Weston McKennie", "MID", "American", 26),
        ("PE001", "Pervis Estupiñán", "DEF", "Colombian", 26),
        ("TA003", "Tariq Lamptey", "DEF", "English", 24),
        ("JA006", "Jan Paul van Hecke", "DEF", "Dutch", 24),
        ("RO004", "Robert Sánchez", "GK", "Spanish", 26),
        ("PA003", "Pascal Groß", "MID", "German", 33),
        ("SI001", "Simon Adingra", "FWD", "Ivorian", 23),
    ],
    # Fulham
    "Fulham": [
        ("RA001", "Raúl Jiménez", "FWD", "Mexican", 33),
        ("AL002", "Alexander Iwobi", "MID", "Nigerian", 28),
        ("MA008", "Máximo Perrone", "MID", "Argentine", 22),
        ("AN003", "Antonee Robinson", "DEF", "American", 25),
        ("RE002", "Reiss Nelson", "FWD", "English", 25),
        ("HE001", "Hector Bellerin", "DEF", "Spanish", 29),
        ("BE003", "Ben Chilwell", "DEF", "English", 27),
        ("GO001", "Goncalo Guedes", "MID", "Portuguese", 27),
        ("LA001", "Leno", "GK", "German", 32),
        ("WI001", "Wilson", "DEF", "Brazilian", 28),
    ],
    # West Ham
    "West Ham": [
        ("DE002", "Declan Rice", "MID", "English", 25),
        ("MU002", "Mohammed Kudus", "MID", "Ghanaian", 24),
        ("JA007", "Jarrod Bowen", "FWD", "English", 27),
        ("LU002", "Luiz Guilherme", "MID", "Brazilian", 20),
        ("MI002", "Michail Antonio", "FWD", "English", 34),
        ("PA004", "Pape Gueye", "MID", "Senegalese", 25),
        ("KU001", "Kurt Zouma", "DEF", "French", 30),
        ("DA003", "Dani Ogbagu", "DEF", "English", 26),
        ("AL003", "Alphonse Areola", "GK", "French", 31),
        ("EM003", "Emicrania Bowen", "DEF", "English", 27),
    ],
    # Everton
    "Everton": [
        ("CA002", "Callum Coady", "DEF", "English", 31),
        ("AL004", "Amadou Onana", "MID", "Belgian", 22),
        ("JA008", "James Tarkowski", "DEF", "English", 31),
        ("MI003", "Mina", "DEF", "Colombian", 30),
        ("JO006", "João Neves", "MID", "Portuguese", 20),
        ("NI002", "Nico O'Neill", "MID", "English", 20),
        ("RI002", "Richarlison", "FWD", "Brazilian", 27),
        ("DA004", "Dwight McNeil", "MID", "English", 26),
        ("JO007", "Jordan Pickford", "GK", "English", 30),
        ("PE002", "Peter Bosz", "MID", "Dutch", 32),
    ],
    # Southampton
    "Southampton": [
        ("JA009", "Jan Bednarek", "DEF", "Polish", 28),
        ("CA003", "Cameron Archer", "FWD", "English", 22),
        ("YU001", "Yukinari Sugawara", "DEF", "Japanese", 24),
        ("KA003", "Kamaldeen Sulemana", "FWD", "Ghanaian", 22),
        ("JO008", "Joe Aribo", "MID", "Nigerian", 28),
        ("GA002", "Gavin Bazunu", "GK", "Irish", 22),
        ("WY001", "Wyles Fleming", "MID", "English", 23),
        ("PE003", "Pereira Shelvey", "MID", "English", 32),
        ("HA002", "Harry Souttar", "DEF", "Scottish", 24),
        ("RI003", "Romain Perraud", "DEF", "French", 27),
    ],
    # Ipswich Town
    "Ipswich Town": [
        ("AL005", "Alisson Becker", "GK", "Brazilian", 32),
        ("OL002", "Omari Hutchinson", "FWD", "English", 21),
        ("JA010", "Jack Clarke", "FWD", "English", 22),
        ("NE002", "Nedum Onuoha", "DEF", "English", 36),
        ("KI001", "Kieran McKenna", "MID", "English", 37),
        ("TR002", "Tomas Xeka", "MID", "Portuguese", 24),
        ("LE002", "Leif Davis", "DEF", "English", 23),
        ("HU001", "Husbauer", "MID", "Czech", 32),
        ("CA004", "Cameron Burgess", "DEF", "English", 27),
        ("Wal001", "Wallace", "DEF", "Brazilian", 23),
    ],
    # Brentford
    "Brentford": [
        ("BR004", "Bryan Mbeumo", "FWD", "Cameroonian", 24),
        ("KO002", "Kristoffer Ajer", "DEF", "Norwegian", 27),
        ("EN002", "Endo Sergio", "MID", "Japanese", 26),
        ("RA002", "Raith Thomas", "DEF", "English", 28),
        ("DA005", "David Raya", "GK", "Spanish", 29),
        ("NI003", "Nico Schira", "MID", "German", 31),
        ("YO002", "Yoane Wissa", "FWD", "Congolese", 26),
        ("MI004", "Mikkel Damsgaard", "MID", "Danish", 27),
        ("RI004", "Rico Henry", "DEF", "English", 28),
        ("JO009", "Josh da Silva", "MID", "English", 26),
    ],
    # Crystal Palace
    "Crystal Palace": [
        ("MI005", "Michael Olise", "MID", "English", 22),
        ("JE001", "Jean-Philippe Mateta", "FWD", "French", 26),
        ("VI003", "Víctor Tomás", "FWD", "Spanish", 29),
        ("CA005", "Cheick Doucouré", "MID", "Malian", 30),
        ("WI002", "Wilfried Zaha", "FWD", "Ivorian", 32),
        ("DA006", "Daniel Munoz", "DEF", "Colombian", 25),
        ("JO010", "Joel Ward", "DEF", "English", 34),
        ("ED002", "Ed de Goey", "GK", "Dutch", 32),
        ("MA009", "Marc Guéhi", "DEF", "English", 24),
        ("PE004", "Pete Phuket", "DEF", "English", 26),
    ],
    # Leicester City
    "Leicester City": [
        ("JA011", "James Maddison", "MID", "English", 27),
        ("YU002", "Youri Tielemans", "MID", "Belgian", 27),
        ("ST001", "Stephy Mavididi", "FWD", "English", 24),
        ("WI003", "Wes Fofana", "DEF", "French", 25),
        ("JO011", "Jonny Evans", "DEF", "Northern Irish", 36),
        ("RIC001", "Ricardo Pereira", "DEF", "Portuguese", 31),
        ("HU002", "Hubert Ilunga", "MID", "Congolese", 22),
        ("DE003", "Dewsbury-Hall", "MID", "English", 26),
        ("CO002", "Conor Coady", "DEF", "English", 31),
        ("DO003", "Dani Pereira", "GK", "Portuguese", 29),
    ],
    # Wolverhampton
    "Wolverhampton": [
        ("GA003", "Gary O'Neil", "MID", "English", 40),
        ("RA003", "Ravel Morrison", "MID", "English", 31),
        ("PO001", "Pedro Neto", "MID", "Portuguese", 24),
        ("SE001", "Sérgio Conceição", "FWD", "Portuguese", 35),
        ("SA001", "Sander Coates", "DEF", "English", 26),
        ("JO012", "João Moutinho", "MID", "Portuguese", 37),
        ("LO001", "Lodi Renan", "DEF", "Brazilian", 27),
        ("JU002", "Jüngün", "DEF", "Turkish", 29),
        ("JO013", "José Sá", "GK", "Portuguese", 31),
        ("TR003", "Trent Alexander-Arnold", "DEF", "English", 26),
    ],
    # Newcastle
    "Newcastle": [
        ("AL006", "Alexander Isak", "FWD", "Swedish", 25),
        ("MI006", "Miguel Almirón", "MID", "Paraguayan", 30),
        ("AN004", "Anthony Gordon", "FWD", "English", 24),
        ("JO014", "João Cancelo", "DEF", "Portuguese", 30),
        ("TI001", "Tino Livramento", "DEF", "English", 22),
        ("DI002", "Dirk Kuyt", "FWD", "Dutch", 38),
        ("BR005", "Bruno Guimarães", "MID", "Brazilian", 27),
        ("SA002", "Sander Berge", "MID", "Norwegian", 26),
        ("NI004", "Nick Pope", "GK", "English", 32),
        ("JO015", "Joelinton", "MID", "Brazilian", 28),
    ],
    # Luton Town
    "Luton Town": [
        ("CA006", "Carlton Morris", "FWD", "English", 30),
        ("PA005", "Pelly Ruddock", "DEF", "English", 28),
        ("RO005", "Robert Sánchez", "GK", "Spanish", 26),
        ("LE003", "Leboard Johnson", "MID", "English", 23),
        ("VI004", "Valerien Ismael", "DEF", "French", 47),
        ("HA003", "Harry Winks", "MID", "English", 29),
        ("TA004", "Tahith Chong", "FWD", "Dutch", 24),
        ("TY001", "Tye Lawrence", "DEF", "English", 22),
        ("RY002", "Ryan Allsop", "GK", "English", 31),
        ("JA012", "Jarrad Branthwaite", "DEF", "English", 22),
    ],
}

# Match dates throughout the season
SEASON_START = datetime(2024, 8, 16)
SEASON_END = datetime(2025, 5, 25)

def generate_realistic_stats(position: str, age: int) -> Dict:
    """Generate realistic season stats based on player position and age."""
    base_appearances = random.randint(20, 38)

    stats = {
        "appearances": base_appearances,
        "minutes_played": base_appearances * random.randint(60, 90),
    }

    # Position-based stat generation
    if position == "GK":
        stats.update({
            "goals": 0,
            "assists": 0,
            "yellow_cards": random.randint(0, 2),
            "red_cards": 0,
            "clean_sheets": random.randint(5, 15),
            "passes_completed": random.randint(500, 1200),
            "tackles_won": random.randint(10, 30),
            "interceptions": random.randint(20, 50),
            "shots_on_target": 0,
            "key_passes": random.randint(0, 5),
            "xg": 0.0,
            "xa": 0.0,
        })
    elif position == "DEF":
        stats.update({
            "goals": random.randint(0, 3),
            "assists": random.randint(0, 4),
            "yellow_cards": random.randint(2, 8),
            "red_cards": random.randint(0, 1),
            "clean_sheets": random.randint(3, 12),
            "passes_completed": random.randint(800, 2000),
            "tackles_won": random.randint(40, 120),
            "interceptions": random.randint(20, 80),
            "shots_on_target": random.randint(0, 5),
            "key_passes": random.randint(5, 30),
            "xg": round(random.uniform(0.0, 1.5), 2),
            "xa": round(random.uniform(0.0, 2.0), 2),
        })
    elif position == "MID":
        stats.update({
            "goals": random.randint(1, 8),
            "assists": random.randint(2, 12),
            "yellow_cards": random.randint(2, 6),
            "red_cards": random.randint(0, 1),
            "clean_sheets": random.randint(0, 8),
            "passes_completed": random.randint(1200, 3000),
            "tackles_won": random.randint(30, 100),
            "interceptions": random.randint(10, 50),
            "shots_on_target": random.randint(10, 50),
            "key_passes": random.randint(20, 80),
            "xg": round(random.uniform(0.5, 5.0), 2),
            "xa": round(random.uniform(1.0, 8.0), 2),
        })
    elif position == "FWD":
        stats.update({
            "goals": random.randint(5, 20),
            "assists": random.randint(2, 10),
            "yellow_cards": random.randint(1, 5),
            "red_cards": random.randint(0, 1),
            "clean_sheets": 0,
            "passes_completed": random.randint(300, 1000),
            "tackles_won": random.randint(5, 30),
            "interceptions": random.randint(0, 20),
            "shots_on_target": random.randint(30, 100),
            "key_passes": random.randint(10, 50),
            "xg": round(random.uniform(2.0, 10.0), 2),
            "xa": round(random.uniform(0.5, 5.0), 2),
        })

    return stats

def generate_players_csv():
    """Generate players.csv with all 200 PL players."""
    players = []
    player_id_counter = 1

    for team, team_players in sorted(PL_PLAYERS.items()):
        for custom_id, name, position, nationality, age in team_players:
            market_value = {
                "GK": random.randint(10, 50),
                "DEF": random.randint(20, 80),
                "MID": random.randint(30, 150),
                "FWD": random.randint(40, 200),
            }[position]

            players.append({
                "player_id": custom_id,
                "name": name,
                "team": team,
                "position": position,
                "nationality": nationality,
                "age": age,
                "market_value_eur": market_value * 1_000_000,
            })

    with open("/sessions/fervent-charming-sagan/pl-scout-hub/data/players.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "player_id", "name", "team", "position", "nationality", "age", "market_value_eur"
        ])
        writer.writeheader()
        writer.writerows(players)

    print(f"Generated {len(players)} players")
    return players

def generate_season_stats_csv(players: List[Dict]):
    """Generate player_season_stats.csv for 2024/25 season."""
    season_stats = []
    stat_id = 1

    for player in players:
        position = player["position"]
        age = player["age"]

        stats = generate_realistic_stats(position, age)

        season_stats.append({
            "stat_id": f"ST{stat_id:04d}",
            "player_id": player["player_id"],
            "season": "2024/25",
            "appearances": stats["appearances"],
            "goals": stats["goals"],
            "assists": stats["assists"],
            "yellow_cards": stats["yellow_cards"],
            "red_cards": stats["red_cards"],
            "minutes_played": stats["minutes_played"],
            "clean_sheets": stats["clean_sheets"],
            "passes_completed": stats["passes_completed"],
            "tackles_won": stats["tackles_won"],
            "interceptions": stats["interceptions"],
            "shots_on_target": stats["shots_on_target"],
            "key_passes": stats["key_passes"],
            "xg": stats["xg"],
            "xa": stats["xa"],
        })
        stat_id += 1

    with open("/sessions/fervent-charming-sagan/pl-scout-hub/data/player_season_stats.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "stat_id", "player_id", "season", "appearances", "goals", "assists",
            "yellow_cards", "red_cards", "minutes_played", "clean_sheets",
            "passes_completed", "tackles_won", "interceptions", "shots_on_target",
            "key_passes", "xg", "xa"
        ])
        writer.writeheader()
        writer.writerows(season_stats)

    print(f"Generated {len(season_stats)} season stat records")
    return season_stats

def generate_match_events_csv(players: List[Dict]):
    """Generate match_events.csv with ~2000 match events."""
    events = []
    event_id = 1
    teams = list(set([p["team"] for p in players]))
    event_types = ["goal", "assist", "yellow_card", "red_card", "substitution"]

    num_matches = 200  # 380 matches total, sample every 2nd
    match_id = 1

    for _ in range(num_matches):
        # Pick random home/away teams
        home_team = random.choice(teams)
        away_team = random.choice([t for t in teams if t != home_team])

        # Random match date
        match_date = SEASON_START + timedelta(days=random.randint(0, (SEASON_END - SEASON_START).days))

        # Realistic score
        home_score = random.randint(0, 4)
        away_score = random.randint(0, 4)

        # Get players from each team
        home_players = [p for p in players if p["team"] == home_team]
        away_players = [p for p in players if p["team"] == away_team]

        # 8-12 events per match
        num_events = random.randint(8, 12)
        for _ in range(num_events):
            event_type = random.choice(event_types)
            minute = random.randint(1, 90)

            if event_type in ["goal", "assist", "yellow_card", "red_card"]:
                player = random.choice(home_players + away_players)
            else:
                player = random.choice(home_players)

            events.append({
                "event_id": f"EV{event_id:05d}",
                "match_id": f"M{match_id:05d}",
                "player_id": player["player_id"],
                "event_type": event_type,
                "minute": minute,
                "match_date": match_date.strftime("%Y-%m-%d"),
                "home_team": home_team,
                "away_team": away_team,
                "home_score": home_score,
                "away_score": away_score,
            })
            event_id += 1

        match_id += 1

    with open("/sessions/fervent-charming-sagan/pl-scout-hub/data/match_events.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "event_id", "match_id", "player_id", "event_type", "minute",
            "match_date", "home_team", "away_team", "home_score", "away_score"
        ])
        writer.writeheader()
        writer.writerows(events)

    print(f"Generated {len(events)} match events across {match_id - 1} matches")

if __name__ == "__main__":
    print("Generating PL Live Scout Hub dataset...")
    players = generate_players_csv()
    generate_season_stats_csv(players)
    generate_match_events_csv(players)
    print("\nDataset generation complete!")
    print(f"  - /sessions/fervent-charming-sagan/pl-scout-hub/data/players.csv")
    print(f"  - /sessions/fervent-charming-sagan/pl-scout-hub/data/player_season_stats.csv")
    print(f"  - /sessions/fervent-charming-sagan/pl-scout-hub/data/match_events.csv")
