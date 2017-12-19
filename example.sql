set yestoday=date_time(current_timestamp(),'minus_day',180);
set max_date=date_time(${hiveconf:yestoday},'day_last_second');
set min_date=date_time(${hiveconf:max_date},'minus_month');

select ${hiveconf:min_date};
select ${hiveconf:max_date};



