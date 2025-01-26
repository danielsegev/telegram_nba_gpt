
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
   ```

2. **Set Up Environment Variables**
   - Create a `.env` file with the following variables:
     - PostgreSQL credentials (e.g., `DWH`, `AIRFLOW`).
     - Kafka broker details.
     - Telegram bot token.
     - ChatGPT API key.

3. **Start the Services**
   ```bash
   docker-compose up -d
   ```

4. **Run Kafka Producers**
   ```bash
   python include/tasks/truncate_postgres_table.py dim_team
   python include/tasks/kafka_producer_teams.py
   python include/tasks/kafka_producer_players.py
   python include/tasks/kafka_producer_games.py
   ```

5. **Run PySpark Consumers**
   ```bash
   spark-submit include/scripts/kafka_to_postgres_teams.py
   spark-submit include/scripts/kafka_to_postgres_players.py
   spark-submit include/scripts/kafka_to_postgres_games.py
   ```

6. **Deploy the Telegram Bot**
   ```bash
   python bot.py
   ```

### **DAG Management**
- **ETL DAGs**:
  - Full ETL pipeline for processing all assets.
  - Separate DAGs for `dim_team`, `dim_player`, and `fact_game`.
- **Task Directory**:
  - All reusable tasks and scripts are located in the `include/tasks` directory, ensuring a clean DAG implementation.

## **Project Features**

### **ETL Pipeline**
- Modular design for processing specific datasets (teams, players, games).
- Reliable and scalable infrastructure using Kafka and PySpark.
- PostgreSQL database optimized for querying structured NBA data.

### **Telegram Bot**
- **ChatGPT Integration**: Enhances user interaction by generating SQL queries dynamically based on user input.
- **Customizable Queries**: Supports advanced SQL queries to fetch detailed insights.
- **Real-Time Updates**: Provides live updates on NBA games, teams, and players.

### **Project Structure**
```
nba_etl_project/
│-- include/
│   │-- tasks/              # Python scripts for reusable tasks
│   │-- scripts/            # PySpark scripts for data processing
│-- dags/                   # Airflow DAGs
│-- bot/                    # Telegram bot implementation
│-- docker-compose.yaml     # Containerized environment configuration
│-- README.md               # Project documentation
```

## **Contributing**
We welcome contributions! Please follow these steps:
1. Fork the repository.
2. Create a new branch (`feature/your-feature`).
3. Make your changes and test them thoroughly.
4. Submit a pull request.

## **License**
This project is open-source and available under the MIT License.

## **Contact**
For questions or troubleshooting, reach out via GitHub Issues or connect with the Telegram bot.

---
