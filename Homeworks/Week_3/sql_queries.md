Create table.
-
```sql
CREATE TABLE dezoomcamp.fhv_tripdata(
    dispatching_base_num String NULL,
    pickup_datetime DateTime,
    dropOff_datetime DateTime,
    PUlocationID Int64 NULL,
    DOlocationID Int64 NULL,
    SR_Flag Int64 NULL,
    Affiliated_base_number String NULL
)
ENGINE = MergeTree
ORDER BY pickup_datetime;
```

Insert data
-
```sql
INSERT INTO dezoomcamp.fhv_tripdata 
SELECT * FROM s3(
    'https://storage.yandexcloud.net/prefect-de-zoomcamp/data/fhv/fhv_tripdata_2019-*.csv.gz', 
    'CSVWithNames'
);
```

Question 1.
-
```sql
SELECT count(pickup_datetime) 
FROM dezoomcamp.fhv_tripdata
```

Question 2.
-
```sql
SELECT DISTINCT Affiliated_base_number 
FROM dezoomcamp.fhv_tripdata
```
```sql
SELECT DISTINCT Affiliated_base_number FROM s3(
    'https://storage.yandexcloud.net/prefect-de-zoomcamp/data/fhv/fhv_tripdata_2019-*.csv.gz', 
    'CSVWithNames'
)
```

Question 3.
-
```sql
SELECT count(pickup_datetime) 
FROM dezoomcamp.fhv_tripdata
WHERE PUlocationID IS NULL AND DOlocationID IS NULL
```

Question 5.
-
```sql
SELECT count(pickup_datetime) 
FROM dezoomcamp.fhv_tripdata
WHERE toDate(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31'
```
