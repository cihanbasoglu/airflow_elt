DECLARE end_dt DATE DEFAULT DATE(current_date);
DECLARE start_dt DATE DEFAULT DATE_SUB(end_dt, interval 44 day);

CREATE TABLE IF NOT EXISTS your_dataset.your_table(
    app_id STRING,
    app_name STRING,
    platform STRING,
    date DATE,
    revenue FLOAT64,
    adnetwork STRING
);

delete from `your_dataset.your_table`  WHERE Date BETWEEN start_dt and end_dt;

insert into `your_dataset.your_table`
select
  cast(fyber_app_id as string) as fyber_app_id,
  app_name,
  platform,
  date(date) as date,
  revenue_usd_ as revenue,
  'DTExchange' as AdNetwork
from `your_dataset.your_raw_table`
where date(Date) between start_dt and end_dt
