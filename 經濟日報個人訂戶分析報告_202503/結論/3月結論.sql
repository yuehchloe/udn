-------- 計算每個月有多少新訂訂單數，並將結果儲存成資料表 new_sub_list --------
-- year_month, all_num, new_sub_num, new_f_sub_num, new_b_sub_num, rt_sub_num, r_g_sub_num 
-- 依序為當月月份、總訂單數、新訂單總數、新訂單首購、新訂單回購、續訂訂單數、退貨訂單數
declare data_time, end_data_time date;
set data_time = DATE('2021-10-01');
set end_data_time = DATE('2025-04-01');

create or replace temp table new_sub_list (
  year_month date,
  all_num int64,
  new_sub_num int64,
  new_f_sub_num int64,
  new_b_sub_num int64,
  rt_sub_num int64,
  r_g_sub_num int64
);

while data_time <= end_data_time do

insert into new_sub_list
select 
  year_month, all_num, new_sub_num, new_f_sub_num, new_b_sub_num, rt_sub_num, r_g_sub_num
from (
  select year_month, count(*) as all_num, 
         sum(new_sub_1) as new_sub_num, 
         sum(new_f_sub) as new_f_sub_num, 
         sum(new_b_sub) as new_b_sub_num, 
         sum(rt_sub) as rt_sub_num
  from (
    select *,
      date_trunc(date(ordertime), month) as year_month,
      if(account in (
          select account 
          from `ddd-bd-analytics.money.money_subscribe_list`  
          where data_month = data_time and new_sub = '1'
      ), 1, 0) as new_sub_1,

      if(account in (
          select account 
          from `ddd-bd-analytics.money.money_subscribe_list`  
          where data_month = data_time and new_sub = '1'
        ) and account not in (
          select account 
          from `ddd-bd-analytics.money.money_subscribe_list`  
          where data_month < data_time
      ), 1, 0) as new_f_sub,

      if(account in (
          select account 
          from `ddd-bd-analytics.money.money_subscribe_list`  
          where data_month = data_time and new_sub = '1'
        ) and account in (
          select account 
          from `ddd-bd-analytics.money.money_subscribe_list`  
          where data_month < data_time
      ), 1, 0) as new_b_sub,

      if(account not in (
          select account 
          from `ddd-bd-analytics.money.money_subscribe_list`  
          where data_month = data_time and new_sub = '1'
      ), 1, 0) as rt_sub
    from (
      select *
      from `ddd-bd-analytics.money.money_subscribe_list`
      where data_month = data_time
    )
  )
  group by year_month
)
left join (
  select 
    count(order_no) as r_g_sub_num, 
    y_month
  from (
    select account, order_no, date_trunc(date(auth_time), month) as y_month
    from `ddd-data-pipeline.rr_synchronized.auth_record`
    where auth_type = '13'
      and product_id = '2'
      and account is not null
      and order_no is not null
  )
  where y_month = data_time
  group by y_month
) on y_month = year_month;

set data_time = date_add(data_time, interval 1 month);
end while;

select * from new_sub_list order by 1;




-------- 計算 MONEY 首購訂戶成為訂戶前一個月的瀏覽行為，並將結果儲存成資料表 first_time_subscribers_view --------
-- 設定分析期間
declare start_date date default '2024-04-01';
declare end_date date default '2025-03-31';

CREATE OR REPLACE TABLE `ddd-bd-analytics.money_monthly.first_time_subscribers_view` AS -- 將結果儲存成資料表

-- 抓出分析期間內的首購訂戶
with first_time_subs_in_range as (
  select 
    account,
    min(date(ordertime)) as first_sub_date
  from `ddd-bd-analytics.money.money_subscribe_list`
  where new_sub = '1'
  group by account
  having first_sub_date between start_date and end_date
),

-- 篩選 GA4 瀏覽資料 (僅限 page_view)
raw_events as (
  select 
    parse_date('%Y%m%d', _table_suffix) as event_date,
    user_id,
    event_name,
    (
      select value.string_value from unnest(event_params)
      where key = "content_level"
    ) as content_level
  from `udnga4.analytics_289594638.events_*`
  where _table_suffix between format_date('%Y%m%d', date_sub(start_date, interval 30 day))
                          and format_date('%Y%m%d', end_date)
    and event_name = 'page_view'
),

-- 加入 content_level 分類
classified_events as (
  select *,
    case 
      when content_level = '限付費訂戶' then '限付費訂戶'
      when content_level = '限會員' then '限會員'
      when content_level = '開放閱讀' then '開放閱讀'
      else '未知'
    end as page_type
  from raw_events
),

-- 有瀏覽行為的帳號：RFV + 內容類型
rfv_content as (
  select
    s.account,
    s.first_sub_date,
    count(distinct e.event_date) as F,
    count(*) as V,
    min(date_diff(s.first_sub_date, e.event_date, day)) as R,
    countif(page_type = '限付費訂戶') as paid_views,
    countif(page_type = '限會員') as member_views,
    countif(page_type = '開放閱讀') as free_views,
    countif(page_type = '未知') as unknown_views
  from first_time_subs_in_range s
  left join classified_events e
    on s.account = e.user_id
    and e.event_date between date_sub(s.first_sub_date, interval 30 day)
                        and date_sub(s.first_sub_date, interval 1 day)
  group by s.account, s.first_sub_date
),

-- 補沒有瀏覽的帳號：用月份天數當作 R
rfv_final as (
  select
    s.account,
    s.first_sub_date,
    ifnull(r.R, extract(day from last_day(date_sub(s.first_sub_date, interval 1 day)))) as R,
    ifnull(r.F, 0) as F,
    ifnull(r.V, 0) as V,
    round(ifnull(r.V, 0) / 30.0, 2) as avg_daily_views,
    ifnull(r.paid_views, 0) as paid_views,
    ifnull(r.member_views, 0) as member_views,
    ifnull(r.free_views, 0) as free_views,
    ifnull(r.unknown_views, 0) as unknown_views
  from first_time_subs_in_range s
  left join rfv_content r
    on s.account = r.account
)

-- 加上 R/F/V 各自分級
select *,
  case 
    when R < 3 then 'High'
    when R <= 8 then 'Medium'
    else 'Low'
  end as R_level,
  case 
    when F > 25 then 'High'
    when F >= 11 then 'Medium'
    else 'Low'
  end as F_level,
  case 
    when V > 135 then 'High'
    when V >= 24 then 'Medium'
    else 'Low'
  end as V_level
from rfv_final
order by first_sub_date, account;





-------- 計算每個月「未造訪就訂閱」的訂戶數，也就是 F = 0（未曾瀏覽）的訂戶人數 --------
with base as (
  select
    date_trunc(first_sub_date, month) as month,
    count(*) as total_subs,
    countif(F = 0) as no_visit_subs
  from `ddd-bd-analytics.money_monthly.first_time_subscribers_view`
  group by month
)
select
  month,
  total_subs,
  no_visit_subs,
  round(no_visit_subs / total_subs, 4) as no_visit_rate_pct
from base
order by month;




-------- 造訪內容層級（content_level）分類的比例 --------
-- 只造訪「限付費訂戶」
-- 只造訪「開放閱讀」
-- 兩者都有造訪（限付費訂戶＋開放閱讀）
-- 只造訪「未知」內容或沒有 content_level 的
with base as (
  select 
    account,
    date_trunc(first_sub_date, month) as month,
    -- 造訪狀況（是否造訪過某類型 content_level）
    paid_views > 0 as has_paid,
    free_views > 0 as has_free,
    unknown_views > 0 and paid_views = 0 and free_views = 0 as only_unknown
  from `ddd-bd-analytics.money_monthly.first_time_subscribers_view`
),

classified as (
  select *,
    case 
      when has_paid and has_free then 'both_paid_and_free'
      when has_paid and not has_free then 'only_paid'
      when has_free and not has_paid then 'only_free'
      when only_unknown then 'only_unknown'
      else 'others'
    end as visit_type
  from base
),

monthly_summary as (
  select 
    month,
    count(*) as total_subs,
    countif(visit_type = 'only_paid') as only_paid_count,
    countif(visit_type = 'only_free') as only_free_count,
    countif(visit_type = 'both_paid_and_free') as both_count,
    countif(visit_type = 'only_unknown') as only_unknown_count
  from classified
  group by month
)

select 
  month,
  total_subs,
  only_paid_count,
  round(only_paid_count / total_subs * 100, 2) as only_paid_pct,
  only_free_count,
  round(only_free_count / total_subs * 100, 2) as only_free_pct,
  both_count,
  round(both_count / total_subs * 100, 2) as both_paid_and_free_pct,
  only_unknown_count,
  round(only_unknown_count / total_subs * 100, 2) as only_unknown_pct
from monthly_summary
order by month;





-------- 加總 RFV 各層級比例（依月份）--------
with base as (
  select 
    account,
    first_sub_date,
    date_trunc(first_sub_date, month) as year_month,
    R_level,
    F_level,
    V_level
  from `ddd-bd-analytics.money_monthly.first_time_subscribers_view`
),

-- 加總 R/F/V 各層級的數量
level_counts as (
  select
    year_month,
    
    countif(R_level = 'High') as R_High,
    countif(R_level = 'Medium') as R_Medium,
    countif(R_level = 'Low') as R_Low,
    
    countif(F_level = 'High') as F_High,
    countif(F_level = 'Medium') as F_Medium,
    countif(F_level = 'Low') as F_Low,
    
    countif(V_level = 'High') as V_High,
    countif(V_level = 'Medium') as V_Medium,
    countif(V_level = 'Low') as V_Low,
    
    count(*) as total_accounts
  from base
  group by year_month
),

-- 計算比例 (%)
level_ratios as (
  select
    year_month,
    
    round(R_High / total_accounts * 100, 1) as R_High_pct,
    round(R_Medium / total_accounts * 100, 1) as R_Medium_pct,
    round(R_Low / total_accounts * 100, 1) as R_Low_pct,
    
    round(F_High / total_accounts * 100, 1) as F_High_pct,
    round(F_Medium / total_accounts * 100, 1) as F_Medium_pct,
    round(F_Low / total_accounts * 100, 1) as F_Low_pct,
    
    round(V_High / total_accounts * 100, 1) as V_High_pct,
    round(V_Medium / total_accounts * 100, 1) as V_Medium_pct,
    round(V_Low / total_accounts * 100, 1) as V_Low_pct,
    
    total_accounts
  from level_counts
)

select * 
from level_ratios
order by year_month;