from database import query_postgresql

print(query_postgresql("SELECT full_name FROM nba_players LIMIT 5;"))
