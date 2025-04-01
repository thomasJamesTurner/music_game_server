CREATE TABLE `p_releases` (
  `id` integer NOT NULL,
  `artist` varchar(255) NOT NULL,
  `title` varchar(255) NOT NULL,
  `country` varchar(41) DEFAULT NULL,
  `genre` varchar(22) DEFAULT NULL,
  `style` varchar(27) DEFAULT NULL,
  `release_date` date NOT NULL,
  `discogs_url` varchar(501) DEFAULT NULL,
  PRIMARY KEY (`id`,`artist`,`release_date`)
)
PARTITION BY LINEAR HASH(`id`) PARTITIONS 400;

