CREATE DATABASE flaskdocker;
USE flaskdocker;

CREATE TABLE `flaskdocker`.`users`(
    `id` INT NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(64),
    PRIMARY KEY (ID)
);