-- SQLite
DROP TABLE IF EXISTS `jobs`;
CREATE TABLE `jobs` (
  job_id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT
);

INSERT INTO `jobs` (`name`) VALUES
    ('job1'),
    ('job2'),
    ('job3'),
    ('job4'),
    ('job5'),
    ('job6'),
    ('job7'),
    ('job8'),
    ('job9');
DELETE FROM jobs WHERE job_id NOT IN (SELECT job_id FROM jobs ORDER BY job_id DESC limit 3);
SELECT * FROM jobs;

INSERT INTO `jobs` (`name`) VALUES
    ('job10'),
    ('job11'),
    ('job12');
DELETE FROM jobs WHERE job_id NOT IN (SELECT job_id FROM jobs ORDER BY job_id DESC limit 3);
SELECT * FROM jobs;
