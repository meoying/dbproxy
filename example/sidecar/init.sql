CREATE DATABASE IF NOT EXISTS order_db;

USE order_db;

DROP TABLE IF EXISTS `orders`;
CREATE TABLE `orders`
(
    `user_id`  int    NOT NULL,
    `order_id` bigint NOT NULL,
    `content`  text,
    `account` double DEFAULT NULL,
    PRIMARY KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
