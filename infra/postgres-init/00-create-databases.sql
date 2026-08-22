-- Runs once when the postgres container's data directory is first
-- initialized. Creates one database per service (service-per-database:
-- no service ever connects directly to another's tables) and applies
-- each service's schema.
--
-- psql's \connect meta-command lets a single init script touch
-- multiple databases; docker's postgres image runs any .sql file
-- placed in /docker-entrypoint-initdb.d through psql, so this works
-- without extra tooling.

CREATE DATABASE fulfillx_order;
CREATE DATABASE fulfillx_inventory;
CREATE DATABASE fulfillx_payment;
CREATE DATABASE fulfillx_fulfillment;

\connect fulfillx_order
\i /docker-entrypoint-initdb.d/order-service-0001.sql.part

\connect fulfillx_inventory
\i /docker-entrypoint-initdb.d/inventory-service-0001.sql.part

\connect fulfillx_payment
\i /docker-entrypoint-initdb.d/payment-service-0001.sql.part

\connect fulfillx_fulfillment
\i /docker-entrypoint-initdb.d/fulfillment-service-0001.sql.part
