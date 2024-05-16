-- --------------------------------------------------------
-- Host:                         127.0.0.1
-- Versión del servidor:         8.0.35 - MySQL Community Server - GPL
-- SO del servidor:              Win64
-- HeidiSQL Versión:             12.4.0.6659
-- --------------------------------------------------------

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8 */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


-- Volcando estructura de base de datos para pub_otc
CREATE DATABASE IF NOT EXISTS `pub_otc` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `pub_otc`;

-- Volcando estructura para tabla pub_otc.pub_otc_hazzard_rates
CREATE TABLE IF NOT EXISTS `pub_otc_hazzard_rates` (
  `id_hazzard` int NOT NULL AUTO_INCREMENT COMMENT 'Id del registro insertado',
  `id_precia` char(30) NOT NULL COMMENT 'Nombre del Hazzard',
  `days` int NOT NULL COMMENT 'Dias',
  `rate` decimal(9,9) NOT NULL COMMENT 'Valor de la tasa Hazzard',
  `valuation_date` date NOT NULL COMMENT 'Fecha de Valoración',
  `status_info` char(1) NOT NULL DEFAULT '1' COMMENT 'Disponibilidad de la información',
  `last_update` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT 'Fecha y hora de la última actualización',
  PRIMARY KEY (`id_hazzard`)
) ENGINE=InnoDB AUTO_INCREMENT=547 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='Contiene la informacion calculada de los hazzard rates';

-- La exportación de datos fue deseleccionada.

/*!40103 SET TIME_ZONE=IFNULL(@OLD_TIME_ZONE, 'system') */;
/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IFNULL(@OLD_FOREIGN_KEY_CHECKS, 1) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40111 SET SQL_NOTES=IFNULL(@OLD_SQL_NOTES, 1) */;
