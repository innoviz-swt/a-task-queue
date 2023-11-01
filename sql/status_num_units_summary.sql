SELECT level, name,
    SUM(num_units) AS total,
    SUM(CASE WHEN status = 'pending' THEN num_units ELSE 0 END) AS pending,
    SUM(CASE WHEN status = 'running' THEN num_units ELSE 0 END) AS running,
    SUM(CASE WHEN status = 'success' THEN num_units ELSE 0 END) AS success,
    SUM(CASE WHEN status = 'failure' THEN num_units ELSE 0 END) AS failure
FROM tasks
GROUP BY level, name;
