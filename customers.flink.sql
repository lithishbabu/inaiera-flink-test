-- FlinkSQL streaming job
--
-- A job is three parts:
--   1. CREATE TABLE for each source  — Kafka topic, CDC stream, …
--   2. CREATE TABLE for each sink    — lakehouse table, Kafka topic, …
--   3. INSERT INTO <sink> SELECT … FROM <source>
--
-- Connector settings go in each table's WITH ( … ) clause.
--
-- ⌘↵          syntax-check this file against the FlinkSQL runner
-- ⌘K          ask the AI chat to generate the pipeline for you
-- Deploy      commit and push, then register the repo under FlinkSQL → Jobs

CREATE TABLE orders_stream (
  order_id   BIGINT,
  customer   INT,
  amount     DOUBLE,
  order_time AS PROCTIME()
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '5',
  'fields.order_id.kind' = 'sequence', 'fields.order_id.start' = '1', 'fields.order_id.end' = '100000',
  'fields.customer.min' = '1', 'fields.customer.max' = '5',
  'fields.amount.min' = '10', 'fields.amount.max' = '500'
);

CREATE TABLE customer_totals (
  customer     INT,
  window_start TIMESTAMP(3),
  total_amount DOUBLE,
  order_count  BIGINT
) WITH ('connector' = 'print');

INSERT INTO customer_totals
SELECT customer,
       TUMBLE_START(order_time, INTERVAL '10' SECOND),
       SUM(amount),
       COUNT(*)
FROM orders_stream
GROUP BY customer, TUMBLE(order_time, INTERVAL '10' SECOND);
