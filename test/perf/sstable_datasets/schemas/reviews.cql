CREATE TABLE bench.reviews (
  parent_asin text,
  review_time timestamp,
  user_id text,
  asin text,
  rating float,
  title text,
  body text,
  images_json text,
  helpful_vote int,
  verified_purchase boolean,
  PRIMARY KEY ((parent_asin), review_time, user_id)
) WITH CLUSTERING ORDER BY (review_time DESC, user_id ASC);
