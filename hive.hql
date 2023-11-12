DROP TABLE IF EXISTS mapreduce_output;
DROP TABLE IF EXISTS regions;
DROP VIEW IF EXISTS avg_price_per_manufacturer;
DROP TABLE IF EXISTS json_res;

CREATE TABLE IF NOT EXISTS mapreduce_output (
  geo_id INT,
  manufacturer STRING,
  num_cars INT,
  total_sales INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

LOAD DATA INPATH '${input_dir3}' INTO TABLE mapreduce_output;

CREATE TABLE IF NOT EXISTS regions (
  geo_id INT,
  region STRING,
  region_url STRING,
  county STRING,
  state_sign STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '^'
STORED AS TEXTFILE;

LOAD DATA INPATH '${input_dir4}' INTO TABLE regions;

CREATE VIEW avg_price_per_manufacturer AS
SELECT r.state_sign, m.manufacturer, SUM(m.total_sales)/SUM(m.num_cars) AS avg_price
FROM mapreduce_output m
JOIN regions r ON m.geo_id = r.geo_id
GROUP BY r.state_sign, m.manufacturer;

CREATE EXTERNAL TABLE IF NOT EXISTS json_res (
    state_sign STRING,
    manufacturer STRING,
    avg_price DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${output_dir6}';

INSERT OVERWRITE TABLE json_res
SELECT 
    state_sign, 
    manufacturer, 
    avg_price
FROM (
    SELECT 
        state_sign, 
        manufacturer, 
        avg_price,
        ROW_NUMBER() OVER (PARTITION BY state_sign ORDER BY avg_price DESC) as rank
    FROM avg_price_per_manufacturer
) ranked
WHERE rank <= 3;