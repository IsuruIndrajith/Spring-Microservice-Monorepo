-- Create application databases and an app user
CREATE DATABASE IF NOT EXISTS inventory_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS order_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER IF NOT EXISTS 'appuser'@'%' IDENTIFIED BY 'apppass';
GRANT ALL PRIVILEGES ON inventory_db.* TO 'appuser'@'%';
GRANT ALL PRIVILEGES ON order_db.* TO 'appuser'@'%';
GRANT ALL PRIVILEGES ON orderdb.* TO 'appuser'@'%';
FLUSH PRIVILEGES;
