-- SQLite
PRAGMA foreign_keys = ON;

INSERT INTO `jobs` (`name`) VALUES
    ('job1'),
    ('job2');

-- SELECT * FROM jobs;

INSERT INTO `tasks` (`job_id`, `name`, `entrypoint`) VALUES
    (1, 'task1', ''),
    (1, 'task2', ''),
    (2, 'task3', ''),
    (2, 'task4', '');

-- Select * FROM tasks;

Select * FROM jobs
    JOIN tasks ON tasks.job_id = jobs.job_id;

DELETE FROM jobs WHERE jobs.job_id = 1;

Select * FROM jobs
    JOIN tasks ON tasks.job_id = jobs.job_id;

Select * FROM tasks;
