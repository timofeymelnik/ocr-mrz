CREATE TABLE IF NOT EXISTS task_queue (
  task_id TEXT PRIMARY KEY,
  task_type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  status TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  max_retries INTEGER NOT NULL DEFAULT 3,
  retry_delay_seconds INTEGER NOT NULL DEFAULT 5,
  available_at INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  idempotency_key TEXT,
  last_error TEXT NOT NULL DEFAULT '',
  result_json TEXT,
  dead_letter_reason TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_task_queue_status_available
ON task_queue(status, available_at);

CREATE INDEX IF NOT EXISTS idx_task_queue_expires_at
ON task_queue(expires_at);

CREATE UNIQUE INDEX IF NOT EXISTS idx_task_queue_idempotency
ON task_queue(idempotency_key)
WHERE idempotency_key IS NOT NULL;
