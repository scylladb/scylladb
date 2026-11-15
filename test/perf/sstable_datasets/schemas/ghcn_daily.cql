CREATE TABLE bench.ghcn_daily (
  station_id text,
  obs_date date,
  element text,
  value int,
  mflag text,
  qflag text,
  sflag text,
  obs_time text,
  PRIMARY KEY ((station_id), obs_date, element)
);
