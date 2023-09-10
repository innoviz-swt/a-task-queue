-- SQLite
DROP TABLE IF EXISTS `test`;

CREATE TABLE IF NOT EXISTS `test` (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  data JSON
);

INSERT INTO `test` (`data`) VALUES
    (NULL),
    ('["a", "b"]'),
    ('["a", "c"]'),
    ('["a", "d"]');

-- Select * FROM test;
Select * FROM test;

UPDATE test
SET data = CASE
    WHEN data IS NULL THEN json('[ "c" ]')
    WHEN data LIKE '%"c"%' THEN data
    ELSE json_insert(data, '$[#]', 'c')
END; -- WHERE id = 2;

SELECT * from test;
