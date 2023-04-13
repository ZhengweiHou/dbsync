DROP TABLE IF EXISTS student;

CREATE TABLE student (
  id int NOT NULL AUTO_INCREMENT,
  `name` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  age int DEFAULT NULL,
  grades decimal(7,2) DEFAULT NULL,
  modified timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id)
);

INSERT INTO student
(name, age, grades)
VALUES('张三', 0, 0);