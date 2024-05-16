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


-- Volcando estructura de base de datos para precia_utils
CREATE DATABASE IF NOT EXISTS `precia_utils` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `precia_utils`;

-- Volcando estructura para tabla precia_utils.precia_utils_hzd_params
CREATE TABLE IF NOT EXISTS `precia_utils_hzd_params` (
  `id_hazzard` int NOT NULL AUTO_INCREMENT COMMENT 'Id del registro insertado',
  `counterparty` char(6) NOT NULL COMMENT 'Nombre de la contraparte',
  `ref_curve` char(6) NOT NULL COMMENT 'Referencia de la curva',
  `spread` decimal(5,4) NOT NULL COMMENT 'Valor del spread',
  `status_info` char(1) NOT NULL DEFAULT '1' COMMENT 'Disponibilidad de la información',
  `last_update` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT 'Fecha y hora de la última actualización',
  PRIMARY KEY (`id_hazzard`)
) ENGINE=InnoDB AUTO_INCREMENT=15 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='Contiene la informacion de los parametros de los Hazzard rates';

-- Volcando datos para la tabla precia_utils.precia_utils_hzd_params: ~13 rows (aproximadamente)
INSERT INTO `precia_utils_hzd_params` (`id_hazzard`, `counterparty`, `ref_curve`, `spread`, `status_info`, `last_update`) VALUES
	(1, 'BAAA2', 'BAAA2', 0.0000, '1', '2024-01-04 14:39:52.934'),
	(2, 'FAAA2', 'BAAA2', 0.1000, '1', '2024-01-04 14:39:52.945'),
	(3, 'RAAA3', 'BAAA2', 0.0000, '1', '2024-01-04 14:39:52.955'),
	(4, 'F60', 'BAAA2', 0.4000, '1', '2024-01-04 14:39:52.965'),
	(5, 'F59', 'BAAA2', 1.2500, '1', '2024-01-04 14:39:52.974'),
	(6, 'F58', 'BAAA2', 2.0000, '1', '2024-01-04 14:39:52.983'),
	(7, 'B60', 'BAAA2', 0.3000, '1', '2024-01-04 14:39:52.992'),
	(8, 'B59', 'BAAA2', 1.1500, '1', '2024-01-04 14:39:52.999'),
	(10, 'R60', 'BAAA2', 0.4875, '1', '2024-01-04 14:39:53.015'),
	(11, 'R59', 'BAAA2', 1.2500, '1', '2024-01-04 14:39:53.026'),
	(12, 'R50', 'BAAA2', 2.0300, '1', '2024-01-04 14:39:53.036'),
	(13, 'R40', 'BAAA2', 3.6000, '1', '2024-01-04 14:39:53.045'),
	(14, 'B58', 'BAAA2', 1.9000, '1', '2024-01-04 15:00:02.402');

/*!40103 SET TIME_ZONE=IFNULL(@OLD_TIME_ZONE, 'system') */;
/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IFNULL(@OLD_FOREIGN_KEY_CHECKS, 1) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40111 SET SQL_NOTES=IFNULL(@OLD_SQL_NOTES, 1) */;
