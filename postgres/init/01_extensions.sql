-- 01_extensions.sql
-- Enable UUID generation (pgcrypto gives us gen_random_uuid())

CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Confirm
SELECT extname, extversion FROM pg_extension ORDER BY extname;