# **NBA ETL Project with Kafka, PySpark, PostgreSQL, and Telegram Bot**

## **Overview**
This project is an end-to-end ETL (Extract, Transform, Load) pipeline designed to process and analyze NBA data. It incorporates cutting-edge tools like Kafka, PySpark, PostgreSQL, and Airflow for efficient data processing. Additionally, a Telegram bot integrated with ChatGPT API provides dynamic querying capabilities, allowing users to retrieve complex insights from the data warehouse.

## **Project Components**

### **1. Infrastructure**
- **Airflow DAGs**: Orchestrate the ETL pipelines. The DAGs are modular:
  - **Full ETL Pipeline**: A comprehensive pipeline that processes all assets (teams, players, and games).
  - **Separate Pipelines**: DAGs for populating individual assets:
    - `dim_team`: Processes team-related data.
    - `dim_player`: Processes player-related data.
    - `fact_game`: Processes game-related data.
- **Tasks Directory**: All Airflow DAG tasks are stored in a separate directory (`include/tasks`), ensuring better organization and reusability.

### **2. ETL Pipeline**
- **Kafka Producers**: Fetch real-time and historical data from the NBA API and publish it to Kafka topics (`nba_teams`, `nba_players`, `nba_games`).
- **PySpark Consumers**: Consume data from Kafka, transform it into a structured format, and load it into PostgreSQL tables.
- **PostgreSQL Database**: Stores structured NBA data in three main tables:
  - `dim_team`
  - `dim_player`
  - `fact_game`

### **3. Telegram Bot with ChatGPT Integration**
- **Dynamic Querying**: The bot is integrated with the ChatGPT API, enabling users to:
  - Write complex SQL queries dynamically.
  - Retrieve insights about teams, players, and games from the PostgreSQL database.
- **User-Friendly Interface**: Interact with the bot via Telegram to fetch real-time insights or historical data.
- **Example Queries**:
  - "Show me the top 5 players by points in the last game."
  - "What is the average height of players in the current season?"

## **Setup Instructions**

### **Prerequisites**
- Docker & Docker Compose installed.
- Python 3.8+ with necessary dependencies.
- Spark and Kafka environments set up.

### **Installation Steps**
1. **Clone the Repository**
   ```bash
   git clone https://github.com/danielsegev/telegram_nba_gpt.git
   cd telegram_nba_gpt
