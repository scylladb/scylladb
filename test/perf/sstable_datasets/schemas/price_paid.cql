CREATE TABLE bench.price_paid (
  postcode text,
  transfer_date date,
  transaction_id uuid,
  price bigint,
  property_type text,
  old_new text,
  duration text,
  paon text,
  saon text,
  street text,
  locality text,
  town text,
  district text,
  county text,
  ppd_category text,
  record_status text,
  PRIMARY KEY ((postcode), transfer_date, transaction_id)
);
