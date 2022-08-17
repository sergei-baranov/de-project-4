-- CDM

DROP TABLE IF EXISTS cdm.dm_courier_ledger;
CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger
(
    id SERIAL CONSTRAINT dm_courier_ledger_pkey PRIMARY KEY,
    courier_id INTEGER NOT NULL,
    courier_name VARCHAR,
    settlement_year SMALLINT NOT NULL,
    settlement_month SMALLINT NOT NULL,
    orders_count INTEGER NOT NULL DEFAULT 0,
    orders_total_sum NUMERIC(14, 2) NOT NULL DEFAULT 0,
    rate_avg NUMERIC(14, 2) NOT NULL DEFAULT 0,
    order_processing_fee NUMERIC(14, 2)
        GENERATED ALWAYS AS (orders_total_sum * 0.25) STORED,
    courier_order_sum NUMERIC(14, 2) NOT NULL DEFAULT 0,
    -- GENERATED ALWAYS AS (
    --     CASE
    --         WHEN rate_avg < 4 THEN
    --             CASE
    --                 WHEN
    --                     orders_total_sum * 0.05 > orders_count * 100
    --                 THEN
    --                     orders_total_sum * 0.05
    --                 ELSE
    --                     orders_count * 100
    --             END
    --         WHEN rate_avg < 4.5 THEN
    --             CASE
    --                 WHEN
    --                     orders_total_sum * 0.07 > orders_count * 150
    --                 THEN
    --                     orders_total_sum * 0.07
    --                 ELSE
    --                     orders_count * 150
    --             END
    --         WHEN rate_avg < 4.9 THEN
    --             CASE
    --                 WHEN
    --                     orders_total_sum * 0.08 > orders_count * 175
    --                 THEN
    --                     orders_total_sum * 0.08
    --                 ELSE
    --                     orders_count * 175
    --             END
    --         ELSE
    --             CASE
    --                 WHEN
    --                     orders_total_sum * 0.1 > orders_count * 200
    --                 THEN
    --                     orders_total_sum * 0.1
    --                 ELSE
    --                     orders_count * 200
    --             END
    --     END
    -- ) STORED,
    courier_tips_sum NUMERIC(14, 2) NOT NULL DEFAULT 0,
    courier_reward_sum NUMERIC(14, 2)
        GENERATED ALWAYS AS (courier_order_sum + courier_tips_sum * 0.95) STORED
);

-- DDS

-- dds dimensions

-- DROP TABLE IF EXISTS dds.dm_timestamps;
CREATE TABLE IF NOT EXISTS dds.dm_timestamps (
    id serial4 NOT NULL CONSTRAINT dm_timestamps_pkey PRIMARY KEY,
    ts timestamp NOT NULL,
    "year" int2 NOT NULL,
    "month" int2 NOT NULL,
    "day" int2 NOT NULL,
    "time" time NOT NULL,
    "date" date NOT NULL,
    CONSTRAINT dm_timestamps_day_check CHECK (((day >= 1) AND (day <= 31))),
    CONSTRAINT dm_timestamps_month_check CHECK (((month >= 1) AND (month <= 12))),
    CONSTRAINT dm_timestamps_pkey PRIMARY KEY (id),
    CONSTRAINT dm_timestamps_ts_uindex UNIQUE (ts),
    CONSTRAINT dm_timestamps_year_check CHECK (((year >= 2022) AND (year < 2500)))
);

-- DROP TABLE IF EXISTS dds.dm_restaurants;
CREATE TABLE IF NOT EXISTS dds.dm_restaurants (
    id serial4 NOT NULL,
    restaurant_id varchar NOT NULL,
    restaurant_name varchar NOT NULL,
    active_from timestamp NOT NULL DEFAULT '1970-01-01 00:00:00'::timestamp without time zone,
    active_to timestamp NOT NULL DEFAULT '2099-12-31 00:00:00'::timestamp without time zone,
    CONSTRAINT dm_restaurants_pkey PRIMARY KEY (id)
);

DROP TABLE IF EXISTS dds.dm_addresses;
CREATE TABLE IF NOT EXISTS dds.dm_addresses (
    id serial4 NOT NULL CONSTRAINT dm_addresses_pkey PRIMARY KEY,
    address VARCHAR NOT NULL
);

DROP TABLE IF EXISTS dds.dm_couriers;
CREATE TABLE IF NOT EXISTS dds.dm_couriers (
    id serial4 NOT NULL CONSTRAINT dm_couriers_pkey PRIMARY KEY,
    "name" VARCHAR NOT NULL,
    active_from timestamp NOT NULL DEFAULT '1970-01-01 00:00:00'::timestamp without time zone,
    active_to timestamp NOT NULL DEFAULT '2099-12-31 00:00:00'::timestamp without time zone
);

-- DROP TABLE IF EXISTS dds.dm_orders;
CREATE TABLE IF NOT EXISTS dds.dm_orders (
    id serial4 NOT NULL,
    user_id int4 NOT NULL,
    restaurant_id int4 NOT NULL,
    timestamp_id int4 NOT NULL,
    order_key varchar NOT NULL,
    order_status varchar NOT NULL,
    CONSTRAINT dm_orders_pkey PRIMARY KEY (id),
    CONSTRAINT dm_orders_order_key_uindex UNIQUE (order_key),
    CONSTRAINT dm_orders_user_id_fkey FOREIGN KEY (user_id)
        REFERENCES dds.dm_users(id) ON UPDATE CASCADE,
    CONSTRAINT dm_orders_restaurant_id_fkey FOREIGN KEY (restaurant_id)
        REFERENCES dds.dm_restaurants(id) ON UPDATE CASCADE,
    CONSTRAINT dm_orders_timestamp_id_fkey FOREIGN KEY (timestamp_id)
        REFERENCES dds.dm_timestamps(id) ON UPDATE CASCADE
);

-- dds facts

DROP TABLE IF EXISTS dds.fct_deliveries;
CREATE TABLE IF NOT EXISTS dds.fct_deliveries (
    id serial4 NOT NULL CONSTRAINT fct_deliveries_pkey PRIMARY KEY,
    order_id int4 NOT NULL,
    courier_id int4 NOT NULL,
    address_id int4 NOT NULL,
    timestamp_id int4 NOT NULL,
    rate SMALLINT NOT NULL DEFAULT 0,
    sum NUMERIC(14,2) NOT NULL DEFAULT 0,
    tip_sum NUMERIC(14,2) NOT NULL DEFAULT 0,
    CONSTRAINT fct_deliveries_order_id_fkey FOREIGN KEY (order_id)
        REFERENCES dds.dm_orders(id) ON UPDATE CASCADE,
    CONSTRAINT fct_deliveries_courier_id_fkey FOREIGN KEY (courier_id)
        REFERENCES dds.dm_couriers(id) ON UPDATE CASCADE,
    CONSTRAINT fct_deliveries_address_id_fkey FOREIGN KEY (address_id)
        REFERENCES dds.dm_addresses(id) ON UPDATE CASCADE,
    CONSTRAINT fct_deliveries_timestamp_id_fkey FOREIGN KEY (timestamp_id)
        REFERENCES dds.dm_timestamps(id) ON UPDATE CASCADE
);

-- STG

DROP TABLE IF EXISTS stg.deliverysystem_deliveries;
CREATE TABLE IF NOT EXISTS stg.deliverysystem_deliveries (
    id serial4 NOT NULL
        CONSTRAINT deliverysystem_deliveries_pkey PRIMARY KEY,
    object_id varchar NOT NULL
        CONSTRAINT deliverysystem_deliveries_object_id_uindex UNIQUE,
    object_value text NOT NULL,
    update_ts timestamp NOT NULL
);

DROP TABLE IF EXISTS stg.deliverysystem_couriers;
CREATE TABLE IF NOT EXISTS stg.deliverysystem_couriers (
    id serial4 NOT NULL
        CONSTRAINT deliverysystem_couriers_pkey PRIMARY KEY,
    object_id varchar NOT NULL
        CONSTRAINT deliverysystem_couriers_object_id_uindex UNIQUE,
    object_value text NOT NULL,
    update_ts timestamp NOT NULL
);

DROP TABLE IF EXISTS stg.deliverysystem_restaurants;
CREATE TABLE IF NOT EXISTS stg.deliverysystem_restaurants (
    id serial4 NOT NULL
        CONSTRAINT deliverysystem_restaurants_pkey PRIMARY KEY,
    object_id varchar NOT NULL
        CONSTRAINT deliverysystem_restaurants_object_id_uindex UNIQUE,
    object_value text NOT NULL,
    update_ts timestamp NOT NULL
);