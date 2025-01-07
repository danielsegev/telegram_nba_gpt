# **NBA ETL Project with Kafka, PySpark, and PostgreSQL**

## **Overview**
This project is an end-to-end ETL (Extract, Transform, Load) pipeline that retrieves NBA data, processes it using Apache Kafka and PySpark, and stores it in PostgreSQL for further analysis. Additionally, it integrates with a Telegram bot to provide insights and updates on NBA games, players, and teams.

## **Project Components**

### **1. Data Sources**
- NBA API: Fetches real-time and historical data on teams, players, and games.
- Kafka: Used as the data streaming platform to manage and distribute data efficiently.

### **2. ETL Process**
- **Producers**: Python scripts fetch NBA data and push it into Kafka topics (`nba_teams`, `nba_players`, `nba_games`).
- **Consumers**: PySpark applications consume Kafka messages, process the data, and insert it into PostgreSQL tables.
- **PostgreSQL Database**: Stores structured NBA data for analysis and querying.

### **3. Infrastructure & Deployment**
- **Docker Compose**: Manages containers for Kafka, PostgreSQL, Spark, and supporting services.
- **Airflow**: Orchestrates data ingestion and transformation processes.
- **Elasticsearch & Kibana**: Provides monitoring and visualization of data pipelines.
- **MinIO**: Object storage for potential data backups and archival.

### **4. Telegram Bot Integration**
- Retrieves insights from the PostgreSQL database.
- Sends real-time updates on NBA games and player stats.
- Allows users to request team/player details via Telegram chat.

## **Setup Instructions**
### **Prerequisites**
- Docker & Docker Compose installed.
- Python 3.8+ with required dependencies.
- Kafka, PostgreSQL, and PySpark setup.

### **Installation Steps**
1. **Clone the Repository**
   ```sh
   git clone https://github.com/danielsegev/telegram_nba_gpt.git
   cd telegram_nba_gpt
   ```
2. **Set Up Environment Variables**
   - Create a `.env` file with necessary configurations (Kafka brokers, PostgreSQL credentials, Telegram bot token, etc.).
3. **Start Services**
   ```sh
   docker-compose up -d
   ```
4. **Run Kafka Producers**
   ```sh
   python producers/teams_producer.py
   python producers/players_producer.py
   python producers/games_producer.py
   ```
5. **Run Kafka Consumers (PySpark Jobs)**
   ```sh
   spark-submit consumers/teams_consumer.py
   spark-submit consumers/players_consumer.py
   spark-submit consumers/games_consumer.py
   ```
6. **Deploy the Telegram Bot**
   ```sh
   python bot.py
   ```

## **Usage**
- The Telegram bot provides real-time NBA updates.
- Query structured NBA data from PostgreSQL.
- Analyze and visualize data with Kibana.

## **Project Structure**
```
telegram_nba_gpt/
│-- producers/          # Kafka producers fetching NBA data
│-- consumers/          # PySpark consumers processing data
│-- database/           # SQL scripts for PostgreSQL schema setup
│-- bot/                # Telegram bot implementation
│-- configs/            # Configuration files and environment variables
│-- logs/               # Logging and monitoring setup
│-- docker-compose.yaml # Containerized environment configuration
│-- README.md           # Project documentation
```

## **Contributing**
Contributions are welcome! Please follow these steps:
1. Fork the repository.
2. Create a new branch.
3. Make your changes and test them.
4. Submit a pull request.

## **License**
This project is open-source and available under the MIT License.

---
For further questions or troubleshooting, feel free to reach out via GitHub Issues or Telegram bot support.

