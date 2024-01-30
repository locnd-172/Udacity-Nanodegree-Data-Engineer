CREATE EXTERNAL TABLE IF NOT EXISTS `locnd15-stedi`.`customer_trusted` (
  `customerName` string,
  `email` string,
  `phone` string,
  `birthday` string,
  `registrationDate` bigint,
  `lastUpdateFate` bigint,
  `shareWithResearchAsOfDate` bigint,
  `shareWithPublicAsOfDate` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://locnd15-lake-house/customer/trusted/'
TBLPROPERTIES ('classification' = 'json');