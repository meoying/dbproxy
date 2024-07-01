CREATE DATABASE IF NOT EXISTS order_db_0;

USE order_db_0;

DROP TABLE IF EXISTS `order_tab`;
CREATE TABLE `order_tab`
(
    `user_id`  int    NOT NULL,
    `order_id` bigint NOT NULL,
    `content`  text,
    `account` double DEFAULT NULL,
    PRIMARY KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE DATABASE IF NOT EXISTS order_db_1;

USE order_db_1;

DROP TABLE IF EXISTS `order_tab`;
CREATE TABLE `order_tab`
(
    `user_id`  int    NOT NULL,
    `order_id` bigint NOT NULL,
    `content`  text,
    `account` double DEFAULT NULL,
    PRIMARY KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


CREATE DATABASE IF NOT EXISTS order_db_2;

USE order_db_2;

DROP TABLE IF EXISTS `order_tab`;
CREATE TABLE `order_tab`
(
    `user_id`  int    NOT NULL,
    `order_id` bigint NOT NULL,
    `content`  text,
    `account` double DEFAULT NULL,
    PRIMARY KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;