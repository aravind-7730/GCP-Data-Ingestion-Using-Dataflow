CREATE TABLE `data-proc-7730.ingest.business` (
  business_id STRING NOT NULL,
  name STRING,
  address STRING,
  city STRING,
  state STRING,
  postal_code STRING,
  latitude FLOAT64,
  longitude FLOAT64,
  stars FLOAT64,
  review_count INT64,
  is_open INT64,
  attributes STRUCT<
    RestaurantsTakeOut BOOL,
    BusinessParking STRUCT<
      garage BOOL,
      street BOOL,
      validated BOOL,
      lot BOOL,
      valet BOOL
    >
  >,
  categories ARRAY<STRING>,
  hours STRUCT<
    Monday STRING,
    Tuesday STRING,
    Wednesday STRING,
    Thursday STRING,
    Friday STRING,
    Saturday STRING,
    Sunday STRING
  >
);


CREATE TABLE `data-proc-7730.ingest.reviews` (
  review_id STRING NOT NULL,
  user_id STRING NOT NULL,
  business_id STRING NOT NULL,
  stars INT64 NOT NULL,
  date DATETIME NOT NULL,
  text STRING,
  useful INT64,
  funny INT64,
  cool INT64
);


CREATE TABLE `data-proc-7730.ingest.error_table` (
  id INT64 NOT NULL,
  error_message STRING,
  timestamp TIMESTAMP
);
