-- Connect as a superuser (e.g., source_user) in the source Postgres database in Docker, then run:

-- 1. Create CDC user
CREATE USER airbyte_cdc_user PASSWORD 'airbyte';

-- 2. Grant replication role to the user
ALTER ROLE airbyte_cdc_user WITH REPLICATION;

-- 3. Grant read access to the database tables in iti schema
GRANT USAGE ON SCHEMA iti TO airbyte_cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA iti TO airbyte_cdc_user;

-- 4. For future tables in iti schema
ALTER DEFAULT PRIVILEGES IN SCHEMA iti GRANT SELECT ON TABLES TO airbyte_cdc_user;

-- 5. Make sure that the parameter wal_level is logical
SHOW wal_level;
-- If not 'logical', change it:
ALTER SYSTEM SET wal_level = logical;

-- Restart the Docker Compose containers, then check:
SHOW wal_level;

-- 6. Make sure that the parameter max_wal_senders > 1
SHOW max_wal_senders;

-- 7. Check replication slots
SHOW max_replication_slots;  -- We only have Airbyte, so at least 1

-- 8. Create a logical replication slot for Airbyte
SELECT pg_create_logical_replication_slot('airbyte_slot', 'pgoutput');

-- 9. Set replication identity for each table in iti schema
ALTER TABLE iti.aircrafts REPLICA IDENTITY DEFAULT;
ALTER TABLE iti.airlines REPLICA IDENTITY DEFAULT;
ALTER TABLE iti.airports REPLICA IDENTITY DEFAULT;
ALTER TABLE iti.customers REPLICA IDENTITY DEFAULT;
ALTER TABLE iti.flight_bookings REPLICA IDENTITY DEFAULT;
ALTER TABLE iti.hotel REPLICA IDENTITY DEFAULT;
ALTER TABLE iti.hotel_bookings REPLICA IDENTITY DEFAULT;

-- 10. Create a publication that monitors all tables in iti schema
CREATE PUBLICATION airbyte_publication FOR TABLE
    iti.aircrafts,
    iti.airlines,
    iti.airports,
    iti.customers,
    iti.flight_bookings,
    iti.hotel,
    iti.hotel_bookings;

-- Optional: to include future tables, Airbyte docs recommend creating separate publications per schema.

-- 11. Enable CDC replication in Airbyte UI
-- In your Postgres source, set replication mode to Logical Replication (CDC),
-- and enter:
-- Replication Slot: airbyte_slot
-- Publication: airbyte_publication

-- 12. Check replication slot and publication
SELECT * FROM pg_replication_slots;
SELECT * FROM pg_publication;