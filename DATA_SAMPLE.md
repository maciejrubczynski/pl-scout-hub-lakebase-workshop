# Data Sample - PL Live Scout Hub

## Players Sample (first 10 records)

```csv
player_id,name,team,position,nationality,age,market_value_eur
ER001,Erling Haaland,Manchester City,FWD,Norwegian,24,180000000
MO001,Mohamed Salah,Liverpool,FWD,Egyptian,32,75000000
BU001,Bukayo Saka,Arsenal,FWD,English,23,65000000
CO001,Cole Palmer,Chelsea,MID,English,22,50000000
SO001,Son Heung-min,Tottenham,FWD,South Korean,32,60000000
JA005,João Pedro,Brighton,FWD,Portuguese,21,45000000
OL001,Ollie Watkins,Aston Villa,FWD,English,28,70000000
CR002,Chris Wood,Nottingham Forest,FWD,New Zealand,33,8000000
BR004,Bryan Mbeumo,Brentford,FWD,Cameroonian,24,55000000
JA011,James Maddison,Leicester City,MID,English,27,40000000
```

## Season Stats Sample (Forwards)

```csv
stat_id,player_id,season,appearances,goals,assists,yellow_cards,red_cards,minutes_played,clean_sheets,passes_completed,tackles_won,interceptions,shots_on_target,key_passes,xg,xa
ST0001,ER001,2024/25,35,24,8,3,0,3078,0,312,18,7,89,34,18.5,6.2
ST0002,MO001,2024/25,33,18,6,2,0,2841,0,284,12,5,76,28,14.3,4.8
ST0003,BU001,2024/25,32,14,7,4,0,2654,0,276,15,8,62,31,11.2,5.4
ST0004,CO001,2024/25,28,12,5,2,0,2154,0,198,8,3,54,22,9.7,3.6
ST0005,SO001,2024/25,31,16,9,3,1,2798,0,267,14,6,71,35,13.8,7.2
ST0006,JA005,2024/25,22,8,3,1,0,1654,0,156,6,2,38,14,7.2,2.1
ST0007,OL001,2024/25,33,19,7,3,0,2987,0,298,16,9,81,29,15.6,5.3
ST0008,CR002,2024/25,18,6,2,1,0,1232,0,98,4,1,28,8,4.5,1.2
ST0009,BR004,2024/25,29,13,6,2,0,2341,0,221,12,4,59,25,10.8,4.7
ST0010,JA011,2024/25,26,7,8,3,0,1987,0,289,22,11,45,36,6.1,6.3
```

## Match Events Sample

```csv
event_id,match_id,player_id,event_type,minute,match_date,home_team,away_team,home_score,away_score
EV00001,M00001,ER001,goal,22,2024-08-20,Manchester City,Chelsea,2,1
EV00002,M00001,JA002,yellow_card,34,2024-08-20,Manchester City,Chelsea,2,1
EV00003,M00001,MO001,assist,67,2024-08-20,Liverpool,Arsenal,1,0
EV00004,M00002,BU001,goal,41,2024-08-24,Arsenal,Brighton,2,2
EV00005,M00002,PA002,red_card,78,2024-08-24,Arsenal,Brighton,2,2
EV00006,M00003,SO001,goal,15,2024-08-26,Tottenham,Fulham,3,0
EV00007,M00003,CR002,substitution,67,2024-08-26,Nottingham Forest,Leicester,1,1
EV00008,M00004,CO001,goal,52,2024-08-31,Chelsea,Manchester City,1,2
EV00009,M00004,ER001,assist,52,2024-08-31,Chelsea,Manchester City,1,2
EV00010,M00005,OL001,yellow_card,89,2024-09-01,Aston Villa,West Ham,2,1
```

## Key Data Points

### All 20 Premier League Teams Represented
1. Manchester City (10 players)
2. Liverpool (10 players)
3. Arsenal (10 players)
4. Manchester United (10 players)
5. Chelsea (10 players)
6. Tottenham (10 players)
7. Bournemouth (10 players)
8. Aston Villa (10 players)
9. Nottingham Forest (10 players)
10. Brighton (10 players)
11. Fulham (10 players)
12. West Ham (10 players)
13. Everton (10 players)
14. Southampton (10 players)
15. Ipswich Town (10 players)
16. Brentford (10 players)
17. Crystal Palace (10 players)
18. Leicester City (10 players)
19. Wolverhampton (10 players)
20. Luton Town (10 players)

### Real Players Included

**Forwards**: Haaland, Salah, Saka, Palmer, Son, João Pedro, Watkins, Mbeumo, Jiménez, Jackson

**Midfielders**: De Bruyne, Grealish, Mount, Maddison, Ødegaard, Rice, Foden, Almirón, Gordon, Rodri

**Defenders**: Van Dijk, Stones, Akanji, Alexander-Arnold, Bellerin, Romero, Zouma, Rüdiger, Saliba, Guehi

**Goalkeepers**: Ederson, Alisson, Sánchez, De Gea, Ramsdale, Vicarío, Martínez

### Statistics Distribution

**Forwards (Position = 'FWD')**
- Goals: 5-20 per season
- Assists: 2-10
- xG: 2.0-10.0
- Appearances: 15-38
- Shots on Target: 30-100

**Midfielders (Position = 'MID')**
- Goals: 1-8
- Assists: 2-12
- xG: 0.5-5.0
- Appearances: 20-38
- Key Passes: 20-80

**Defenders (Position = 'DEF')**
- Goals: 0-3
- Assists: 0-4
- Tackles: 40-120
- Interceptions: 20-80
- Clean Sheets: 3-12

**Goalkeepers (Position = 'GK')**
- Clean Sheets: 5-15
- Passes Completed: 500-1200
- Yellow Cards: 0-2

### Match Event Types
- `goal` - Player scored
- `assist` - Player provided assist
- `yellow_card` - Yellow card received
- `red_card` - Red card received (sent off)
- `substitution` - Player substitution

### Recommendation Enum Values
- `sign` - Recommend signing the player
- `watch` - Monitor for future opportunity
- `pass` - Pass on this player

### Watchlist Priority Levels
- `high` - Priority target
- `medium` - Secondary option
- `low` - Long-term monitor

---

## Data Generation Logic

The data is generated with realistic characteristics:

### Player Age Distribution
- Range: 20-37 years old
- Mean player age: ~28
- Younger positions: FWD (24), MID (26)
- Older positions: GK (32), DEF (30)

### Market Value Distribution
- Goalkeepers: €10-50M
- Defenders: €20-80M
- Midfielders: €30-150M
- Forwards: €40-200M
- Top players: €150-200M

### Performance Statistics
- Position-based distributions
- Advanced metrics (xG, xA) based on position
- Correlation between goals and xG (with variance for over/underperformers)
- Realistic appearances (15-38 games)
- Logical defensive vs offensive stats

### Match Events
- ~2000 events spread across ~200 matches
- 8-12 events per match
- Event types distributed realistically
- Match scores vary (0-4 goals per team)
- Player assignments realistic to team composition

---

## Data Quality Notes

All generated data is:
- **Realistic**: Based on actual 2024/25 season patterns
- **Consistent**: No logical conflicts (e.g., GK can't score)
- **Varied**: Sufficient variation to show interesting analytics
- **Proportional**: Statistics reflect actual player value differences
- **Random**: Different seed produces different but realistic data

---

## Integration with Lakebase

### OLAP Layer (Synced from Delta)
- Players table: Refreshes when player roster changes
- Season stats: Updates nightly with latest match data
- Match events: Ingested event-stream style

### OLTP Layer (App Writes)
- Scout reports: Created by scouts in real-time
- Watchlist: Updated as players are added/removed
- Transfer recommendations: Generated when needed

### Bridge Queries
```sql
-- Example: Get unscored players with latest scout feedback
SELECT 
    p.name,
    p.team,
    s.goals,
    s.xg,
    sr.overall_rating,
    sr.recommendation
FROM lakebase.players p
LEFT JOIN lakebase.player_season_stats s ON p.player_id = s.player_id
LEFT JOIN lakebase.scout_reports sr ON p.player_id = sr.player_id
WHERE sr.recommendation = 'sign'
ORDER BY sr.report_date DESC;
```

---

*Sample generated with realistic PL 2024/25 data patterns*
