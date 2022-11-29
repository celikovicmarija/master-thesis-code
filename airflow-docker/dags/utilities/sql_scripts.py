create_air_quality_table = '''
CREATE TABLE IF NOT EXISTS `air_quality` (
  `measurement_id` bigint NOT NULL AUTO_INCREMENT,
  `date` date NOT NULL,
  `pm25` smallint DEFAULT NULL,
  `pm10` smallint DEFAULT NULL,
  `o3` smallint DEFAULT NULL,
  `no2` smallint DEFAULT NULL,
  `so2` smallint DEFAULT NULL,
  `co` smallint DEFAULT NULL,
  `monitoring_station_id` tinyint NOT NULL,
  UNIQUE KEY `measurement_id` (`measurement_id`),
  KEY `air_quality_ibfk_1` (`monitoring_station_id`),
  CONSTRAINT `air_quality_ibfk_1` FOREIGN KEY (`monitoring_station_id`) REFERENCES `monitoring_station` (`monitoring_station_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=41603 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
'''
create_exchange_rates_table = '''
CREATE TABLE IF NOT EXISTS `exchange_rates` (
  `exchange_rate_id` bigint NOT NULL AUTO_INCREMENT,
  `price_EUR_USD` decimal(20,4) DEFAULT NULL,
  `exchange_date` date NOT NULL,
  `price_EUR_RSD` decimal(20,4) DEFAULT NULL,
  UNIQUE KEY `exchange_rate_id` (`exchange_rate_id`)
) ENGINE=InnoDB AUTO_INCREMENT=5862 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;'''

create_real_estate_table = '''
CREATE TABLE IF NOT EXISTS `real_estate_post` (
  `post_id` bigint NOT NULL AUTO_INCREMENT,
  `additional` text CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci,
  `city` varchar(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `city_lines` varchar(500) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `date` date DEFAULT NULL,
  `description` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `floor_number` varchar(20) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `heating_type_id` tinyint DEFAULT '13',
  `link` text CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci,
  `location` varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `micro_location` varchar(100) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `monthly_bills` decimal(18,0) DEFAULT NULL,
  `number_of_rooms` varchar(3) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `object_state` varchar(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `object_type` varchar(50) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `price` decimal(18,0) DEFAULT NULL,
  `price_per_unit` decimal(18,0) DEFAULT NULL,
  `real_estate_type_id` tinyint DEFAULT NULL,
  `size` decimal(18,0) DEFAULT NULL,
  `street` varchar(200) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `title` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `total_number_of_floors` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `transaction_type_id` tinyint DEFAULT NULL,
  `is_listed` tinyint(1) DEFAULT NULL,
  `source_id` tinyint DEFAULT NULL,
  `measurement_id` tinyint DEFAULT NULL,
  `geocode_id` bigint DEFAULT NULL,
  PRIMARY KEY (`post_id`),
  KEY `source_id` (`source_id`),
  KEY `real_estate_posts_FK` (`real_estate_type_id`),
  KEY `real_estate_posts_FK_1` (`transaction_type_id`),
  KEY `real_estate_posts_FK_2` (`measurement_id`),
  KEY `real_estate_posts_FK_3` (`heating_type_id`),
  KEY `real_estate_post_FK` (`geocode_id`),
  CONSTRAINT `real_estate_post_FK` FOREIGN KEY (`geocode_id`) REFERENCES `geocode` (`geocode_id`),
  CONSTRAINT `real_estate_post_ibfk_1` FOREIGN KEY (`source_id`) REFERENCES `source` (`source_id`),
  CONSTRAINT `real_estate_posts_FK` FOREIGN KEY (`real_estate_type_id`) REFERENCES `real_estate_type` (`real_estate_type_id`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `real_estate_posts_FK_1` FOREIGN KEY (`transaction_type_id`) REFERENCES `transaction_type` (`transaction_type_id`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `real_estate_posts_FK_2` FOREIGN KEY (`measurement_id`) REFERENCES `size_measurement` (`measurement_id`) ON DELETE RESTRICT ON UPDATE RESTRICT,
  CONSTRAINT `real_estate_posts_FK_3` FOREIGN KEY (`heating_type_id`) REFERENCES `heating_type` (`heating_type_id`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
'''
