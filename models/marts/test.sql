SELECT 
DATE_PART(year, departure_ts) AS departure_year,
DATE_PART(month, departure_ts) AS departure_month,
DATE_PART(day, departure_ts) AS departure_day,
DATE_PART(dayofweek, departure_ts) AS departure_day_of_week,
CASE WHEN 
    DATE_PART(dayofweek, departure_ts) BETWEEN 6 AND 7
    THEN TRUE
    ELSE FALSE
END AS departure_weekend,
DATE_PART(year, return_ts) AS return_year,
DATE_PART(month, return_ts) AS return_month,
DATE_PART(day, return_ts) AS return_day,
DATE_PART(dayofweek, return_ts) AS return_day_of_week,
CASE WHEN 
    DATE_PART(dayofweek, return_ts) BETWEEN 6 AND 7
    THEN TRUE
    ELSE FALSE
END AS return_weekend
FROM raw_data