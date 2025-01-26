import openai
from dotenv import load_dotenv
import os
from database import query_postgresql

# Load environment variables from the .env file
load_dotenv()

# Retrieve the OpenAI API key from the environment variable
openai.api_key = os.getenv("OPENAI_API_KEY")

def process_query_with_chatgpt(user_query):
    # System prompt with stricter guidelines for generating SQL queries
    system_prompt = """
    You are a smart assistant with access to an NBA stats database. All tables are in the 'public' schema.
    Query the PostgreSQL database for relevant information and summarize the results.

    Use the following guidelines:
    1. Always prefix tables with 'public.' (e.g., SELECT * FROM public.dim_team).
    2. Only include relevant columns.
    3. Use LIMIT to restrict the number of rows if no specific filter is provided.
    4. For Boolean fields use 'N' or 'Y'
    You have access to 'dim_team', 'dim_player', and 'fact_game' tables in the 'dwh' database, schema 'public'.
    Here are the columns list for each table (each column refers to its real meaning. All columns names are readable). When generating the SQL query, please use only relevant columns from the relevant tables. Here is a full reference of the data:
    table 'dim_team', columns: team_id (PK, can be joined with nba_players.teamid), abbreviation, city, full_name, state (in the USA), year_founded
    table 'dim_player', columns: id (PK, can be joined with nba_games.personaid), full_name, birthdate, school, country, last_affiliation, height, weight, season_exp, jersey, "position", roster_status, games_played_current_season_flag, team_id, team_name, dleague_flag (does the player plays in the D-League), nba_flag (does the player plays in the NBA), games_played_flag, draft_year (active since), draft_round, draft_number, greatest_75_flag, is_active
    table 'fact_game', columns: status, "order", personid, starter (whether a player started in the game), oncourt, played (whether the player played in this game), statistics_assists (total assists), statistics_blocks (total blocks), statistics_blocksreceived (total blocks received), statistics_fieldgoalsattempted (total field goal attempts), statistics_fieldgoalsmade (total field goals made), statistics_foulsoffensive (total offensive fouls), statistics_foulsdrawn (total fouls drawn), statistics_foulspersonal (total personal fouls), statistics_foulstechnical (total technical fouls), statistics_freethrowsattempted (total free throws attempts), statistics_freethrowsmade (total free throws made), statistics_minus (plus minus - minus), statistics_minutes (total minutes), statistics_minutescalculated (total minutes played), statistics_plus (plus minus - plus), statistics_plusminuspoints (total plus minus), statistics_points (total points), statistics_pointsfastbreak (total points in fast breaks), statistics_pointsinthepaint (total points in the paint), statistics_pointssecondchance (total points by second chance), statistics_reboundsdefensive (toital defensive rebounds), statistics_reboundsoffensive (total offensive rebounds), statistics_reboundstotal (total rebounds), statistics_steals (total steals), statistics_threepointersattempted (total three points attempts), statistics_threepointersmade (total three points made), statistics_turnovers (total turnovers), statistics_twopointersattempted (total two points attempts), statistics_twopointersmade (total two points made), game_id
    When generating SQL queries, ensure the syntax is valid for PostgreSQL.
    Respond with only the SQL query in plain text.
    In most cases you'll need to join the personaID from the fact_game table with the id of the dim_player table.
    """

    try:
        # Use OpenAI's ChatCompletion API to generate a SQL query
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_query}
            ]
        )

        # Extract the generated SQL query
        generated_query = response['choices'][0]['message']['content']

        # Log the generated SQL query for debugging
        print(f"Generated SQL Query: {generated_query}")

        # Validate the generated query (it must start with SELECT)
        if not generated_query.strip().lower().startswith("select"):
            return "The generated query doesn't seem valid. Please rephrase your question."

        # Query the PostgreSQL database
        db_result = query_postgresql(generated_query)

        # If the query execution fails
        if isinstance(db_result, str) and db_result.startswith("Database query failed:"):
            return f"Error executing query: {db_result}"

        # Format the database response using ChatGPT
        formatted_response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "Format the database results into user-friendly text."},
                {"role": "user", "content": str(db_result)}
            ]
        )

        # Return the formatted response
        return formatted_response['choices'][0]['message']['content']

    except Exception as e:
        # Handle unexpected errors gracefully
        print(f"Error in process_query_with_chatgpt: {e}")
        return f"Sorry, an error occurred: {e}"
