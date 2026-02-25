CREATE INDEX IF NOT EXISTS idx_task_queue_dead_letter
ON task_queue(status, updated_at)
WHERE status = 'dead_letter';
