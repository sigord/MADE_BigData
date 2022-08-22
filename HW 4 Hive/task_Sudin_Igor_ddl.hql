ADD JAR /usr/local/hive/lib/hive-serde.jar;

-- USE bdmade2022q2_sudin;

-- logs_row TABLE

DROP TABLE IF EXISTS logs_row;

CREATE EXTERNAL TABLE logs_row(
    ip STRING,
    `date` STRING,
    request STRING,
    page_size INT,
    http_status INT,
    user_agent STRING
)
ROW FORMAT
    serde 'org.apache.hadoop.hive.serde2.RegexSerDe'
    with serdeproperties (
        "input.regex" = "^(\\S*)\\t*(\\d*)\\t(\\S*)\\t(\\d*)\\t(\\d*)\\t(\\S*).*"
    )
STORED AS textfile
LOCATION '/data/user_logs/user_logs_M';

-- users TABLE

DROP TABLE IF EXISTS users;

CREATE EXTERNAL TABLE users (
    ip STRING,
    browser STRING,
    sex STRING,
    age INT
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
LOCATION '/data/user_logs/user_data_M';

-- ip-regions TABLE

DROP TABLE IF EXISTS ip_regions;

CREATE EXTERNAL TABLE ip_regions (
    ip STRING,
    region STRING
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
LOCATION '/data/user_logs/ip_data_M';

-- create logs TABLE

set hive.exec.max.dynamic.partitions.pernode=116;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS logs;

CREATE EXTERNAL TABLE logs(
    ip STRING,
    request STRING,
    page_size INT,
    http_status INT,
    user_agent STRING
) PARTITIONED BY (`date` STRING)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE logs PARTITION(`date`) SELECT ip, request, page_size, http_status, user_agent, SUBSTRING(`date`,1,8) AS `date` FROM logs_row;

-- SELECT * FROM logs_row LIMIT 10;
-- SELECT * FROM users LIMIT 10;
-- SELECT * FROM ip_regions LIMIT 10;