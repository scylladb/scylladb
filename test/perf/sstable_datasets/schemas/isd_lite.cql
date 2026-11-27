CREATE TABLE bench.isd_lite (
  station_id text,
  obs_time timestamp,
  air_temp int,
  dew_point int,
  sea_level_pressure int,
  wind_direction int,
  wind_speed int,
  sky_condition int,
  precip_1h int,
  precip_6h int,
  PRIMARY KEY ((station_id), obs_time)
);
