CREATE TABLE schema_version(version INTEGER PRIMARY KEY);
CREATE TABLE jobs(
  job_id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT,
  description TEXT,
  priority REAL DEFAULT 0
);
CREATE TABLE sqlite_sequence(name,seq);
CREATE TABLE state_kwargs(
  state_kwargs_id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT,
  entrypoint TEXT NOT NULL,
  targs MEDIUMBLOB,
  description TEXT,
  job_id INTEGER NOT NULL,
  CONSTRAINT uq_name_job_id UNIQUE(name, job_id),
  CONSTRAINT fk_job_id FOREIGN KEY(job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
);
CREATE TABLE tasks(
  task_id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT,
  level REAL,
  entrypoint TEXT NOT NULL,
  targs MEDIUMBLOB,
  status TEXT CHECK(status in("pending", "running", "success", "failure")),
  take_time DATETIME,
  start_time DATETIME,
  done_time DATETIME,
  pulse_time DATETIME,
  description TEXT,
  job_id INTEGER NOT NULL,
  CONSTRAINT fk_job_id FOREIGN KEY(job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
);
