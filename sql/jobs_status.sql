SELECT jobs.jid, jobs.name, jobs.priority, jobs.description,
COUNT(*) as tasks,
SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending,
SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running,
SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS success,
SUM(CASE WHEN status = 'failure' THEN 1 ELSE 0 END) AS failure
FROM jobs
LEFT JOIN tasks ON jobs.jid = tasks.jid
ORDER BY jobs.priority DESC;
