-- Airflow's metadata database, kept separate from `findata` so orchestration
-- bookkeeping (DAG runs, task instances, XComs) never mixes with pipeline data.
-- Reuses this Postgres instance rather than adding a second one - at this
-- scale a dedicated Airflow database server would be pure overhead.
--
-- NOTE: files in docker-entrypoint-initdb.d only execute on the FIRST boot of
-- an empty postgres volume. If you add this to a stack whose volume already
-- exists, create the database by hand instead:
--   docker exec findata-postgres psql -U findata -d postgres -c "CREATE DATABASE airflow;"
CREATE DATABASE airflow;
