CREATE TABLE IF NOT EXISTS _asyncpg_queue_schema_version (
    date_applied TIMESTAMP DEFAULT NOW(),
    latest BOOL NOT NULL,
    version TEXT NOT NULL
);

COMMENT ON TABLE _asyncpg_queue_schema_version IS 'Database versions that have been applied for asyncpg-queue';
COMMENT ON COLUMN _asyncpg_queue_schema_version.date_applied IS 'Date at which the version was applied';
COMMENT ON COLUMN _asyncpg_queue_schema_version.latest IS 'Indicator of the most recent version';
COMMENT ON COLUMN _asyncpg_queue_schema_version.version IS 'Name of the version';

INSERT INTO _asyncpg_queue_schema_version (version, latest) VALUES ('20230111', True);

CREATE TABLE IF NOT EXISTS queue (
    id UUID PRIMARY KEY DEFAULT GEN_RANDOM_UUID(),
    enqueued TIMESTAMP DEFAULT NOW(),
    dequeued TIMESTAMP,
    name TEXT,
    data JSONB
);

COMMENT ON TABLE queue IS 'Collection of tasks to process asynchronously';
COMMENT ON COLUMN queue.enqueued IS 'Time task was enqueued';
COMMENT ON COLUMN queue.dequeued IS 'Time task was popped from queue to begin processing';
COMMENT ON COLUMN queue.name IS 'The task to be perfomed';
COMMENT ON COLUMN queue.data IS 'Deserializable data to be passed to the process performing the queued task';

CREATE INDEX IF NOT EXISTS queue_enqueued_idx ON queue (enqueued) WHERE dequeued IS NULL;
CREATE INDEX IF NOT EXISTS queue_dequeued_idx ON queue (dequeued) WHERE dequeued IS NULL;
CREATE INDEX IF NOT EXISTS queue_name_idx ON queue (name) WHERE dequeued IS NULL;

CREATE OR REPLACE FUNCTION _asyncpg_queue_notify() RETURNS TRIGGER AS $$ BEGIN
    PERFORM pg_notify('asyncpg_queue_task'::TEXT, '');
    RETURN NULL;
END $$
LANGUAGE PLPGSQL;

COMMENT ON FUNCTION _asyncpg_queue_notify IS 'Notify channel used by asyncpg-queue';

CREATE OR REPLACE TRIGGER _asyncpg_queue_insert
AFTER INSERT ON queue
FOR EACH ROW
EXECUTE PROCEDURE _asyncpg_queue_notify();

COMMENT ON TRIGGER _asyncpg_queue_insert ON queue IS 'When a new task is inserted into the queue table notify the channel used by asyncpg-queue';
