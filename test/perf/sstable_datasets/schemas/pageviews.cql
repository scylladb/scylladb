CREATE TABLE bench.pageviews (
  domain_code text,
  page_title text,
  hour timestamp,
  views int,
  PRIMARY KEY ((domain_code, page_title), hour)
);
