SELECT 'CREATE DATABASE metabase WITH ENCODING ''UTF8''
       LC_COLLATE ''en_US.UTF-8''
       LC_CTYPE ''en_US.UTF-8''
       TEMPLATE template0;'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'metabase'
)\gexec

DO $$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'metabase') THEN
      CREATE USER metabase WITH PASSWORD 'metabase';
   END IF;
END
$$;

GRANT ALL PRIVILEGES ON DATABASE metabase TO metabase;

SELECT 'CREATE DATABASE airflow WITH ENCODING ''UTF8''
       LC_COLLATE ''en_US.UTF-8''
       LC_CTYPE ''en_US.UTF-8''
       TEMPLATE template0;'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'airflow'
)\gexec

DO $$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow') THEN
      CREATE USER airflow WITH PASSWORD 'airflow';
   END IF;
END
$$;

GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

\c airflow
GRANT ALL ON SCHEMA public TO airflow;

SELECT 'CREATE DATABASE grafana WITH ENCODING ''UTF8''
       LC_COLLATE ''en_US.UTF-8''
       LC_CTYPE ''en_US.UTF-8''
       TEMPLATE template0;'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'grafana'
)\gexec

DO $$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana') THEN
      CREATE USER grafana WITH PASSWORD 'grafana';
   END IF;
END
$$;

GRANT ALL PRIVILEGES ON DATABASE grafana TO grafana;

\c grafana
GRANT ALL ON SCHEMA public TO grafana;

-- SELECT 'CREATE DATABASE spark WITH ENCODING ''UTF8''
--        LC_COLLATE ''en_US.UTF-8''
--        LC_CTYPE ''en_US.UTF-8''
--        TEMPLATE template0;'
-- WHERE NOT EXISTS (
--     SELECT FROM pg_database WHERE datname = 'spark'
-- )\gexec

-- DO $$
-- BEGIN
--    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'spark') THEN
--       CREATE USER spark WITH PASSWORD 'spark';
--    END IF;
-- END
-- $$;

-- GRANT ALL PRIVILEGES ON DATABASE spark TO spark;

-- \c spark

-- CREATE TABLE IF NOT EXISTS spark_logs (
--     id SERIAL PRIMARY KEY,
--     level VARCHAR(10),
--     logger_name TEXT,
--     message TEXT,
--     thread_name TEXT,
--     application_name TEXT,
--     created_at TIMESTAMP DEFAULT NOW()
-- );

-- GRANT ALL ON SCHEMA public TO spark;

-- SELECT 'CREATE DATABASE openmetadata WITH ENCODING ''UTF8''
--        LC_COLLATE ''en_US.UTF-8''
--        LC_CTYPE ''en_US.UTF-8''
--        TEMPLATE template0;'
-- WHERE NOT EXISTS (
--     SELECT FROM pg_database WHERE datname = 'openmetadata'
-- )\gexec

-- DO $$
-- BEGIN
--    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'openmd') THEN
--       CREATE USER openmd WITH PASSWORD 'openmd';
--    END IF;
-- END
-- $$;

-- GRANT ALL PRIVILEGES ON DATABASE openmetadata TO openmd;

-- \c openmetadata
-- GRANT ALL ON SCHEMA public TO openmd;