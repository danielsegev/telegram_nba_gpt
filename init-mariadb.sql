CREATE DATABASE IF NOT EXISTS metastore_db;
CREATE USER IF NOT EXISTS 'admin'@'%' IDENTIFIED BY 'admin';
GRANT ALL PRIVILEGES ON metastore_db.* TO 'admin'@'%';
FLUSH PRIVILEGES;