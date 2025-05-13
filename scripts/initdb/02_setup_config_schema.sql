--##############################################################################
--# Schema for PGLumiLineage Application Configuration
--##############################################################################

CREATE SCHEMA IF NOT EXISTS lumi_config;

COMMENT ON SCHEMA lumi_config IS 'Schema to store PGLumiLineage application-level configurations, such as data source connection details.';

--##############################################################################
--# Table: lumi_config.data_sources
--# Stores connection and metadata information for source databases
--# that PGLumiLineage will monitor.
--##############################################################################
CREATE TABLE IF NOT EXISTS lumi_config.data_sources (
    source_id SERIAL PRIMARY KEY, -- Unique identifier for the data source configuration
    source_name TEXT NOT NULL UNIQUE, -- User-defined unique name for this source (e.g., 'tpcds_prod', 'erp_staging')
    source_type TEXT NOT NULL DEFAULT 'postgresql', -- Type of the data source, defaults to 'postgresql'
    description TEXT, -- Optional description of the data source

    is_active BOOLEAN DEFAULT TRUE NOT NULL, -- Whether this source configuration is currently active for monitoring

    -- Connection details for accessing the source database itself (e.g., for metadata validation)
    db_host TEXT,
    db_port INTEGER,
    db_name TEXT, -- The actual database name on the source server (e.g., 'tpcds')
    db_user TEXT,
    db_password TEXT, -- Consider encrypting this in a production environment or using secrets management

    -- Log retrieval method and parameters
    log_retrieval_method TEXT NOT NULL CHECK (log_retrieval_method IN ('local_path', 'ssh', 'kafka_topic')), -- 'local_path', 'ssh', 'kafka_topic'
    
    -- For 'local_path' method
    log_path_pattern TEXT, -- e.g., '/mnt/pg_logs/tpcds_logs/postgresql-*.csv'

    -- For 'ssh' method
    ssh_host TEXT,
    ssh_port INTEGER DEFAULT 22,
    ssh_user TEXT,
    ssh_password TEXT, -- Alternative to ssh_key_path, consider encryption
    ssh_key_path TEXT, -- Path to the private SSH key for key-based authentication
    ssh_remote_log_path_pattern TEXT, -- e.g., '/var/log/postgresql/postgresql-*.csv' on the remote server

    -- For 'kafka_topic' method (placeholder for future use)
    kafka_bootstrap_servers TEXT,
    kafka_topic TEXT,
    kafka_consumer_group TEXT,

    -- Other metadata
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);

COMMENT ON TABLE lumi_config.data_sources IS 'Stores connection and metadata for source databases PGLumiLineage monitors.';
COMMENT ON COLUMN lumi_config.data_sources.source_id IS 'Auto-incrementing primary key.';
COMMENT ON COLUMN lumi_config.data_sources.source_name IS 'Unique, human-readable name for the data source (e.g., tpcds_prod). This is what our captured_logs.source_database_name will refer to.';
COMMENT ON COLUMN lumi_config.data_sources.source_type IS 'Type of the database (e.g., postgresql, mysql).';
COMMENT ON COLUMN lumi_config.data_sources.description IS 'Optional textual description of the source.';
COMMENT ON COLUMN lumi_config.data_sources.is_active IS 'Flag to enable/disable monitoring for this source.';
COMMENT ON COLUMN lumi_config.data_sources.db_host IS 'Hostname or IP of the source database server (for direct DB connection).';
COMMENT ON COLUMN lumi_config.data_sources.db_port IS 'Port of the source database server.';
COMMENT ON COLUMN lumi_config.data_sources.db_name IS 'Actual database name on the source server (e.g., tpcds).';
COMMENT ON COLUMN lumi_config.data_sources.db_user IS 'Username for connecting to the source database (e.g., for metadata queries).';
COMMENT ON COLUMN lumi_config.data_sources.db_password IS 'Password for the db_user. Store securely in production!';
COMMENT ON COLUMN lumi_config.data_sources.log_retrieval_method IS 'Method to retrieve logs: local_path, ssh, kafka_topic.';
COMMENT ON COLUMN lumi_config.data_sources.log_path_pattern IS 'Log file pattern if method is local_path.';
COMMENT ON COLUMN lumi_config.data_sources.ssh_host IS 'Hostname or IP for SSH connection.';
COMMENT ON COLUMN lumi_config.data_sources.ssh_port IS 'Port for SSH connection (default 22).';
COMMENT ON COLUMN lumi_config.data_sources.ssh_user IS 'Username for SSH connection.';
COMMENT ON COLUMN lumi_config.data_sources.ssh_password IS 'Password for SSH user (alternative to key-based auth). Store securely!';
COMMENT ON COLUMN lumi_config.data_sources.ssh_key_path IS 'Path to the SSH private key file for key-based authentication.';
COMMENT ON COLUMN lumi_config.data_sources.ssh_remote_log_path_pattern IS 'Log file pattern on the remote server, accessed via SSH.';
COMMENT ON COLUMN lumi_config.data_sources.kafka_bootstrap_servers IS 'Kafka bootstrap servers (for future use).';
COMMENT ON COLUMN lumi_config.data_sources.kafka_topic IS 'Kafka topic name where logs are published (for future use).';
COMMENT ON COLUMN lumi_config.data_sources.kafka_consumer_group IS 'Kafka consumer group for PGLumiLineage (for future use).';
COMMENT ON COLUMN lumi_config.data_sources.created_at IS 'Timestamp of when this configuration was created.';
COMMENT ON COLUMN lumi_config.data_sources.updated_at IS 'Timestamp of the last update to this configuration.';



-- Indexes
CREATE INDEX IF NOT EXISTS idx_data_sources_is_active ON lumi_config.data_sources (is_active);

-- Grant permissions (assuming lumiadmin owns this, lumiuser needs to read it)
-- This should be part of your 01_setup_schemas_and_tables.sql or a new setup script for lumi_config
GRANT USAGE ON SCHEMA lumi_config TO lumiuser;
GRANT SELECT ON TABLE lumi_config.data_sources TO lumiuser;
-- (And if lumiuser needs to update status, then 
GRANT UPDATE(is_active, updated_at) ON TABLE lumi_config.data_sources TO lumiuser; 
-- For now, let's assume lumiuser only reads. Admin would update these settings.

--##############################################################################
--# Table: lumi_config.source_sync_schedules
--# Defines the synchronization schedule and parameters for each active data source.
--##############################################################################
CREATE TABLE IF NOT EXISTS lumi_config.source_sync_schedules (
    schedule_id SERIAL PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES lumi_config.data_sources(source_id) ON DELETE CASCADE, -- Foreign key to data_sources table

    is_schedule_active BOOLEAN DEFAULT TRUE NOT NULL, -- Whether this specific schedule entry is active
    
    sync_frequency_type TEXT NOT NULL CHECK (sync_frequency_type IN ('interval', 'cron', 'manual')), -- Type of schedule
    
    -- For 'interval' type: run every X seconds/minutes/hours
    sync_interval_seconds INTEGER CHECK (sync_interval_seconds > 0), -- e.g., 300 for every 5 minutes

    -- For 'cron' type: standard cron expression
    cron_expression TEXT CHECK (sync_frequency_type != 'cron' OR (cron_expression IS NOT NULL AND LENGTH(cron_expression) > 0)), -- e.g., '0 * * * *' for hourly at minute 0

    -- For 'manual' type: no automatic scheduling, only triggered on demand

    last_sync_attempt_at TIMESTAMPTZ, -- Timestamp of the last synchronization attempt for this source
    last_sync_success_at TIMESTAMPTZ, -- Timestamp of the last successful synchronization
    last_sync_status TEXT, -- e.g., 'SUCCESS', 'FAILED', 'IN_PROGRESS'
    last_sync_message TEXT, -- Optional message from the last sync (e.g., error details or records processed)

    -- Concurrency control for this specific source's sync job
    max_concurrent_runs INTEGER DEFAULT 1 CHECK (max_concurrent_runs > 0),

    -- Priority (if multiple jobs are pending, higher priority runs first)
    priority INTEGER DEFAULT 0, -- Lower number means lower priority

    -- Time window for allowed synchronization (e.g., only sync during off-peak hours)
    allowed_sync_start_time TIME, -- e.g., '22:00:00'
    allowed_sync_end_time TIME,   -- e.g., '06:00:00'
    -- Note: TIME window logic needs careful implementation if it spans across midnight.

    config_overrides JSONB, -- Optional JSONB to override specific parameters from data_sources for this schedule
                           -- e.g., different log_path_pattern for a specific scheduled run

    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL -- Application needs to update this
);

COMMENT ON TABLE lumi_config.source_sync_schedules IS 'Defines synchronization schedules for data sources.';
COMMENT ON COLUMN lumi_config.source_sync_schedules.schedule_id IS 'Auto-incrementing primary key for the schedule entry.';
COMMENT ON COLUMN lumi_config.source_sync_schedules.source_id IS 'Foreign key referencing the data_sources table.';
COMMENT ON COLUMN lumi_config.source_sync_schedules.is_schedule_active IS 'Whether this particular schedule configuration is active.';
COMMENT ON COLUMN lumi_config.source_sync_schedules.sync_frequency_type IS 'Type of scheduling: interval, cron, or manual.';
COMMENT ON COLUMN lumi_config.source_sync_schedules.sync_interval_seconds IS 'Polling interval in seconds if sync_frequency_type is interval.';
COMMENT ON COLUMN lumi_config.source_sync_schedules.cron_expression IS 'Cron expression if sync_frequency_type is cron.';
COMMENT ON COLUMN lumi_config.source_sync_schedules.last_sync_attempt_at IS 'Timestamp of the last attempt to run this sync schedule.';
COMMENT ON COLUMN lumi_config.source_sync_schedules.last_sync_success_at IS 'Timestamp of the last successful completion of this sync schedule.';
COMMENT ON COLUMN lumi_config.source_sync_schedules.last_sync_status IS 'Status of the last sync job (e.g., SUCCESS, FAILED).';
COMMENT ON COLUMN lumi_config.source_sync_schedules.last_sync_message IS 'Details or error message from the last sync job.';
COMMENT ON COLUMN lumi_config.source_sync_schedules.max_concurrent_runs IS 'Maximum number of concurrent sync jobs allowed for this source via this schedule (typically 1).';
COMMENT ON COLUMN lumi_config.source_sync_schedules.priority IS 'Job priority for scheduling.';
COMMENT ON COLUMN lumi_config.source_sync_schedules.allowed_sync_start_time IS 'Start of the allowed time window for synchronization.';
COMMENT ON COLUMN lumi_config.source_sync_schedules.allowed_sync_end_time IS 'End of the allowed time window for synchronization.';
COMMENT ON COLUMN lumi_config.source_sync_schedules.config_overrides IS 'JSONB for schedule-specific parameter overrides from the main data_sources config.';
COMMENT ON COLUMN lumi_config.source_sync_schedules.created_at IS 'Timestamp of when this schedule entry was created.';
COMMENT ON COLUMN lumi_config.source_sync_schedules.updated_at IS 'Timestamp of the last update to this schedule entry (managed by application).';

-- Unique constraint to ensure one active schedule definition per source of a certain type,
-- or allow multiple manual/differently configured schedules.
-- This might need adjustment based on business logic (e.g., can a source have multiple active interval schedules?)
-- For simplicity, let's assume one primary schedule per source for now or make source_id + type unique
CREATE UNIQUE INDEX IF NOT EXISTS uq_source_id_active_schedule ON lumi_config.source_sync_schedules (source_id) WHERE is_schedule_active = TRUE;
-- If you want to allow multiple active schedules (e.g. one cron, one interval) for the same source, remove the above or make it more complex.

-- Indexes
CREATE INDEX IF NOT EXISTS idx_source_sync_schedules_source_id ON lumi_config.source_sync_schedules (source_id);
CREATE INDEX IF NOT EXISTS idx_source_sync_schedules_is_active_status ON lumi_config.source_sync_schedules (is_schedule_active, last_sync_status);
CREATE INDEX IF NOT EXISTS idx_source_sync_schedules_next_run ON lumi_config.source_sync_schedules (is_schedule_active, sync_frequency_type, last_sync_success_at) WHERE is_schedule_active = TRUE;

-- ----------------------------------------------------------------------------
-- 设置权限
-- ----------------------------------------------------------------------------

-- 将 lumi_config schema 的所有者设置为 lumiadmin
ALTER SCHEMA lumi_config OWNER TO lumiadmin;

-- 将 lumi_config schema 中的所有表的所有者设置为 lumiadmin
DO $$
DECLARE
    obj RECORD;
BEGIN
    FOR obj IN 
        SELECT tablename FROM pg_tables WHERE schemaname = 'lumi_config'
    LOOP
        EXECUTE format('ALTER TABLE lumi_config.%I OWNER TO lumiadmin', obj.tablename);
    END LOOP;
END $$;

-- 将 lumi_config schema 中的所有序列的所有者设置为 lumiadmin
DO $$
DECLARE
    obj RECORD;
BEGIN
    FOR obj IN 
        SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = 'lumi_config'
    LOOP
        EXECUTE format('ALTER SEQUENCE lumi_config.%I OWNER TO lumiadmin', obj.sequence_name);
    END LOOP;
END $$;

-- 授予 lumiuser 对 lumi_config schema 的使用权限
GRANT USAGE ON SCHEMA lumi_config TO lumiuser;

-- 授予 lumiuser 对 lumi_config.data_sources 表的权限
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE lumi_config.data_sources TO lumiuser;

-- 授予 lumiuser 对 lumi_config.source_sync_schedules 表的权限
GRANT SELECT ON TABLE lumi_config.source_sync_schedules TO lumiuser;
GRANT UPDATE (last_sync_attempt_at, last_sync_success_at, last_sync_status, last_sync_message, updated_at) ON TABLE lumi_config.source_sync_schedules TO lumiuser;

-- 授予 lumiuser 对 lumi_config schema 中所有序列的权限
DO $$
DECLARE
    obj RECORD;
BEGIN
    FOR obj IN 
        SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = 'lumi_config'
    LOOP
        EXECUTE format('GRANT USAGE, SELECT, UPDATE ON SEQUENCE lumi_config.%I TO lumiuser', obj.sequence_name);
    END LOOP;
END $$;

-- 为 lumi_config schema 设置默认权限
ALTER DEFAULT PRIVILEGES IN SCHEMA lumi_config FOR ROLE lumiadmin
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO lumiuser;

ALTER DEFAULT PRIVILEGES IN SCHEMA lumi_config FOR ROLE lumiadmin
GRANT USAGE, SELECT ON SEQUENCES TO lumiuser;