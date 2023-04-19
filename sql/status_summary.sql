SELECT level, 
    COUNT(*) as total,
    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending,
    SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS success,
    SUM(CASE WHEN status = 'failure' THEN 1 ELSE 0 END) AS failure
FROM tasks 
GROUP BY level;
