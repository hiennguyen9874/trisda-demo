CREATE DATABASE IF NOT EXISTS trisda;
USE trisda;
CREATE TABLE IF NOT EXISTS `tris_road_count` (
    `deviceid` varchar(256) COMMENT "",
	`window_start` DATETIME COMMENT "",
	`window_end` DATETIME COMMENT "",
    `motorbike` INT NULL COMMENT "",
    `bus` INT NULL COMMENT "",
    `car` INT NULL COMMENT "",
    `truck` INT NULL COMMENT "",
    `pedestrian` INT NULL COMMENT "",
    `vehicle_others` INT NULL COMMENT "",
    `container_truck` INT NULL COMMENT ""
)
PRIMARY KEY (deviceid, window_start, window_end)
DISTRIBUTED BY HASH (window_start)
;
