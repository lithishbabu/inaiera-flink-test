-- FlinkSQL streaming job: orders per city, in 10-second windows.
--
-- Source : a built-in generator ("datagen"), so it needs no Kafka and runs anywhere: about 5 fake orders per second,
--          each with a city number from 1 to 4.
-- Logic  : the city number becomes a city name, then each city's orders are summed, counted and averaged per 10-second window.
-- Sink   : "print" (the Flink task manager log), so you can watch the results appear every 10 seconds.
--
-- Register it under FlinkSQL -> Jobs with SQL path:  jobs/orders_by_city.flink.sql

CREATE TABLE orders_stream (
  order_id   BIGINT,
  city_id    INT,
  amount     DOUBLE,
  order_time AS PROCTIME()
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '5',
  'fields.order_id.kind' = 'sequence', 'fields.order_id.start' = '1', 'fields.order_id.end' = '100000',
  'fields.city_id.min' = '1', 'fields.city_id.max' = '4',
  'fields.amount.min' = '10', 'fields.amount.max' = '500'
);

CREATE TABLE city_totals (
  city         STRING,
  window_start TIMESTAMP(3),
  total_amount DOUBLE,
  order_count  BIGINT,
  avg_amount   DOUBLE
) WITH ('connector' = 'print');

INSERT INTO city_totals
SELECT CASE city_id WHEN 1 THEN 'Hyderabad' WHEN 2 THEN 'Mumbai' WHEN 3 THEN 'Chennai' ELSE 'Delhi' END,
       TUMBLE_START(order_time, INTERVAL '10' SECOND),
       SUM(amount),
       COUNT(*),
       AVG(amount)
FROM orders_stream
GROUP BY city_id, TUMBLE(order_time, INTERVAL '10' SECOND);
