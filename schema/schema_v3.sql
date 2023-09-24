CREATE TABLE schema_version(version INTEGER PRIMARY KEY);
CREATE TABLE jobs(
  jid INTEGER PRIMARY KEY,
  name TEXT,
  description TEXT,
  priority REAL DEFAULT 0
);
CREATE TABLE tasks(
  tid INTEGER PRIMARY KEY,
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
  jid INTEGER NOT NULL,
  FOREIGN KEY(jid) REFERENCES jobs(jid)
);
