CREATE database if not exists `dbproxy`;

use `dbproxy`;

CREATE TABLE users
(
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(50) NOT NULL
);

CREATE TABLE `test_int_type`
(
    `id`             int NOT NULL,
    `type_tinyint`   tinyint   DEFAULT NULL,
    `type_smallint`  smallint  DEFAULT NULL,
    `type_mediumint` mediumint DEFAULT NULL,
    `type_int`       int       DEFAULT NULL,
    `type_integer`   int       DEFAULT NULL,
    `type_bigint`    bigint    DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `test_float_type`
(
    `id`         int DEFAULT NULL,
    `type_float` float(10, 5
) DEFAULT NULL,
    `type_double` double(10,5) DEFAULT NULL,
    `type_decimal` decimal(10,2) DEFAULT NULL,
    `type_numeric` decimal(10,2) DEFAULT NULL,
    `type_real` double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `test_string_type`
(
    `id`              int          DEFAULT NULL,
    `type_char`       char(10)     DEFAULT NULL,
    `type_varchar`    varchar(255) DEFAULT NULL,
    `type_tinytext`   tinytext,
    `type_text`       text,
    `type_mediumtext` mediumtext,
    `type_longtext`   longtext,
    `type_enum`       enum('small','medium','large') DEFAULT NULL,
    `type_set` set('a','b','c','d') DEFAULT NULL,
    `type_binary`     binary(10) DEFAULT NULL,
    `type_varbinary`  varbinary(255) DEFAULT NULL,
    `type_json`       json         DEFAULT NULL,
    `type_bit`        bit(10)      DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `test_date_type`
(
    `id`             int      DEFAULT NULL,
    `type_date`      date     DEFAULT NULL,
    `type_datetime`  datetime DEFAULT NULL,
    `type_timestamp` timestamp NULL DEFAULT NULL,
    `type_time`      time     DEFAULT NULL,
    `type_year` year DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `test_geography_type`
(
    `id`                   int             DEFAULT NULL,
    `type_geometry`        geometry        DEFAULT NULL,
    `type_geomcollection`  geomcollection  DEFAULT NULL,
    `type_linestring`      linestring      DEFAULT NULL,
    `type_multilinestring` multilinestring DEFAULT NULL,
    `type_point`           point           DEFAULT NULL,
    `type_multipoint`      multipoint      DEFAULT NULL,
    `type_polygon`         polygon         DEFAULT NULL,
    `type_multipolygon`    multipolygon    DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `test_file_path_type`
(
    `id`              int DEFAULT NULL,
    `type_tinyblob`   tinyblob,
    `type_mediumblob` mediumblob,
    `type_blob`       blob,
    `type_longblob`   longblob
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO `test_int_type` (`id`, `type_tinyint`, `type_smallint`, `type_mediumint`, `type_int`, `type_integer`,
                             `type_bigint`)
VALUES (1, 1, 2, 3, 4, 5, 6);
INSERT INTO `test_int_type` (`id`, `type_tinyint`, `type_smallint`, `type_mediumint`, `type_int`, `type_integer`,
                             `type_bigint`)
VALUES (2, 127, 32767, 8388607, 2147483647, 2147483647, 9223372036854775807);
INSERT INTO `test_int_type` (`id`, `type_tinyint`, `type_smallint`, `type_mediumint`, `type_int`, `type_integer`,
                             `type_bigint`)
VALUES (3, -128, -32768, -8388608, -2147483648, -2147483648, -9223372036854775808);
INSERT INTO `test_int_type` (`id`, `type_tinyint`, `type_smallint`, `type_mediumint`, `type_int`, `type_integer`,
                             `type_bigint`)
VALUES (4, NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO `test_float_type` (`id`, `type_float`, `type_double`, `type_decimal`, `type_numeric`, `type_real`)
VALUES
    (1, 66.66000, 999.99900, 33.33, 123456.78, 12345.6789),
    (2, -99999.99999, -99999.99999, -99999999.99, -99999999.99, -1.7976931348623157E+308),
    (3, 99999.99999, 99999.99999, 99999999.99, 99999999.99, 1.7976931348623157E+308),
    (4, NULL, NULL, NULL, NULL, NULL);

INSERT INTO `test_string_type` (`id`, `type_char`, `type_varchar`, `type_tinytext`, `type_text`, `type_mediumtext`,
                                `type_longtext`, `type_enum`, `type_set`, `type_binary`, `type_varbinary`, `type_json`,
                                `type_bit`)
VALUES (1, '一', '二', '三', '四', '五', '六', 'small', 'b,c', 0x61626300000000000000, 0x616263646566, '{
  \"age\": 25, \"name\": \"Tom\", \"address\": {\"city\": \"New York\", \"zipcode\": \"10001\"}}', b'0010101010');
INSERT INTO `test_string_type` (`id`, `type_char`, `type_varchar`, `type_tinytext`, `type_text`, `type_mediumtext`,
                                `type_longtext`, `type_enum`, `type_set`, `type_binary`, `type_varbinary`, `type_json`,
                                `type_bit`)
VALUES (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO `test_date_type` (`id`, `type_date`, `type_datetime`, `type_timestamp`, `type_time`, `type_year`)
VALUES (1, '2024-05-25', '2024-05-25 23:51:00', '2024-05-25 23:51:05', '23:51:08', 2024);
INSERT INTO `test_date_type` (`id`, `type_date`, `type_datetime`, `type_timestamp`, `type_time`, `type_year`)
VALUES (2, NULL, NULL, NULL, NULL, NULL);

INSERT INTO `test_geography_type` (`id`, `type_geometry`, `type_geomcollection`, `type_linestring`,
                                   `type_multilinestring`, `type_point`, `type_multipoint`, `type_polygon`,
                                   `type_multipolygon`)
VALUES (1, ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)'),
        ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(0 0, 1 1, 2 2))'),
        ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)'),
        ST_GeomFromText('MULTILINESTRING((0 0, 1 1, 2 2), (2 2, 3 3, 4 4))'), ST_GeomFromText('POINT(40.7128 -74.006)'),
        ST_GeomFromText('MULTIPOINT(40.7128 -74.006, 34.0522 -118.2437)'),
        ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'),
        ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((20 20, 20 30, 30 30, 30 20, 20 20)))'));
INSERT INTO `test_geography_type` (`id`, `type_geometry`, `type_geomcollection`, `type_linestring`,
                                   `type_multilinestring`, `type_point`, `type_multipoint`, `type_polygon`,
                                   `type_multipolygon`)
VALUES (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO `test_file_path_type` (`id`, `type_tinyblob`, `type_mediumblob`, `type_blob`, `type_longblob`)
VALUES (1, 0x01020304FFFFFFFF0000000CAACB0000, 0x01020304FFFFFFFF0000000CAACB0000, 0x01020304FFFFFFFF0000000CAACB0000,
        0x01020304FFFFFFFF0000000CAACB0000);
INSERT INTO `test_file_path_type` (`id`, `type_tinyblob`, `type_mediumblob`, `type_blob`, `type_longblob`)
VALUES (2, NULL, NULL, NULL, NULL);
