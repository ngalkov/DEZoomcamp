Question 3. Count records
-
```sql
select count(*) as "count"
from green_taxi_data
where cast(lpep_pickup_datetime as date) = '2019-01-15' and 
      cast(lpep_dropoff_datetime as date) = '2019-01-15'
```

Question 4. Largest trip for each day
-
```sql
select lpep_pickup_datetime, trip_distance
from green_taxi_data
order by trip_distance desc
limit 10;
```

Question 5. The number of passengers
-
```sql
select count(*), passenger_count
from green_taxi_data
where cast(lpep_pickup_datetime as date) = '2019-01-01' and 
--    cast(lpep_dropoff_datetime as date) = '2019-01-01' and
      passenger_count in (2, 3)
group by passenger_count;
```

Question 6. Largest tip
-
```sql
select zpu."Zone", zdo."Zone", tip_amount
from
    green_taxi_data trips
    join zones zpu on trips."PULocationID" = zpu."LocationID"
    join zones zdo on trips."DOLocationID" = zdo."LocationID"
where zpu."Zone" = 'Astoria'
order by 
    tip_amount desc
limit 10;
```
