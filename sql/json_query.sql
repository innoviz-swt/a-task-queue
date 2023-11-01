-- SQLite
DROP TABLE IF EXISTS `test`;

CREATE TABLE IF NOT EXISTS `test` (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  `category` varchar(64) NOT NULL,
  data JSON
);

INSERT INTO `test` (`category`, `data`) VALUES
    ('test1', '{"a":1,"b":5}'),
    ('test1', '{"a":2,"b":6}'),
    ('test2', '{"a":3,"b":7}'),
    ('test2', '{"a":4,"b":8, "c": 3}');


Select * FROM test;

-- Select json_extract(data, '$.a') FROM test;

SELECT id,
    SUM(json_extract(data, '$.a')) as a,
    SUM(json_extract(data, '$.b')) as b,
    SUM(json_extract(data, '$.c')) as c
FROM test
GROUP BY category;
