-- FlinkSQL streaming job: spot high-value orders as they arrive (no windows, no waiting).
--
-- Source : a built-in generator ("datagen"), so it needs no Kafka and runs anywhere: about 5 fake orders per second.
-- Logic  : keep only orders of 300 or more and label each one a tier: silver (300+), gold (350+) or platinum (450+).
-- Sink   : "print" (the Flink task manager log): one line per qualifying order, immediately.
--
-- Register it under FlinkSQL -> Jobs with SQL path:  jobs/high_value_orders.flink.sql

CREATE TABLE orders_stream (
  order_id   BIGINT,
  customer   INT,
  amount     DOUBLE,
  order_time AS PROCTIME()
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '5',
  'fields.order_id.kind' = 'sequence', 'fields.order_id.start' = '1', 'fields.order_id.end' = '100000',
  'fields.customer.min' = '1', 'fields.customer.max' = '20',
  'fields.amount.min' = '10', 'fields.amount.max' = '500'
);

CREATE TABLE high_value_orders (
  order_id  BIGINT,
  customer  INT,
  amount    DOUBLE,
  tier      STRING
) WITH ('connector' = 'print');

INSERT INTO high_value_orders
SELECT order_id,
       customer,
       amount,
       CASE WHEN amount >= 450 THEN 'platinum'
            WHEN amount >= 350 THEN 'gold'
            ELSE 'silver' END
FROM orders_stream
WHERE amount >= 300;
