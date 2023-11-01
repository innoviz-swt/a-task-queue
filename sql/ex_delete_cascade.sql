-- SQLite
PRAGMA foreign_keys = ON;
DROP TABLE IF EXISTS `wheels`;
DROP TABLE IF EXISTS `doors`;
DROP TABLE IF EXISTS `cars`;

-- Car

CREATE TABLE IF NOT EXISTS `cars` (
  car_id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT
);

INSERT INTO `cars` (`name`) VALUES
    ('bmw'),
    ('wolkswagen'),
    ('toyota'),
    ('honda');

-- SELECT * FROM cars;

-- Wheels
CREATE TABLE IF NOT EXISTS `wheels` (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    car_id INTEGER,
    CONSTRAINT fk_car_id
        FOREIGN KEY (car_id)
        REFERENCES cars(car_id)
        ON DELETE CASCADE
);

INSERT INTO `wheels` (`car_id`, `name`) VALUES
    (1, 'front left'),
    (1, 'front right'),
    (2, 'back left'),
    (2, 'back right');

-- Select * FROM wheels;

-- Dooss
CREATE TABLE IF NOT EXISTS `doors` (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    car_id INTEGER,
    CONSTRAINT fk_car_id
        FOREIGN KEY (car_id)
        REFERENCES cars(car_id)
        ON DELETE CASCADE
);

INSERT INTO `doors` (`car_id`, `name`) VALUES
    (1, 'right door'),
    (1, 'left door'),
    (2, 'left door'),
    (2, 'right door');

-- Select * FROM doors;

Select * FROM cars
    JOIN wheels ON cars.car_id = wheels.car_id
    JOIN doors ON cars.car_id = doors.car_id;

DELETE FROM cars WHERE cars.car_id = 1;

Select * FROM cars
    JOIN wheels ON cars.car_id = wheels.car_id
    JOIN doors ON cars.car_id = doors.car_id;

Select * FROM doors;
Select * FROM wheels;
