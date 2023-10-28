-- SQLite
DROP TABLE IF EXISTS `test`;

CREATE TABLE IF NOT EXISTS `test` (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT,
  value INTEGER,
  CONSTRAINT uq_name UNIQUE(value, name)
);

INSERT INTO `test` (`name`, `value`) VALUES
    ('name1', 1),
    ('name1', 2),
    ('name2', 2);

SELECT * FROM `test`;

INSERT INTO `test` (`name`, `value`) VALUES
    ('name2', 1);

SELECT * FROM `test`;

INSERT INTO `test` (`name`, `value`) VALUES
    ('name2', 2);
