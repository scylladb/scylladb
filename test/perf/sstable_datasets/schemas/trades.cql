CREATE TABLE bench.trades (
  symbol text,
  hour_bucket timestamp,
  trade_id bigint,
  price double,
  qty double,
  quote_qty double,
  trade_time timestamp,
  is_buyer_maker boolean,
  is_best_match boolean,
  PRIMARY KEY ((symbol, hour_bucket), trade_id)
);
