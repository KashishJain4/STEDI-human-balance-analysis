create external table customer_landing(
    birthDay string,
    customerName string,
    email string,
    lastUpdateDate bigint,
    phone string,
    registrationDate bigint, 
    serialNumber string,
    shareWithFriendsAsOfDate bigint,
    shareWithPublicAsOfDate bigint,
    shareWithResearchAsOfDate bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-kashish-project/customer_landing/';
