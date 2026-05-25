create external table accelerometer_landing(
    timestamp bigint, 
    user string,
    x double,
    y double,
    z double 
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-kashish-project/accelerometer_landing/';
