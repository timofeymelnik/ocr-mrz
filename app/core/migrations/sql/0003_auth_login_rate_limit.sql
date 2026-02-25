CREATE TABLE IF NOT EXISTS auth_login_attempts (
  email TEXT NOT NULL,
  client_ip TEXT NOT NULL,
  failed_attempts INTEGER NOT NULL DEFAULT 0,
  first_failed_at INTEGER NOT NULL DEFAULT 0,
  last_failed_at INTEGER NOT NULL DEFAULT 0,
  locked_until INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (email, client_ip)
);

CREATE INDEX IF NOT EXISTS idx_auth_login_attempts_locked_until
ON auth_login_attempts(locked_until);
