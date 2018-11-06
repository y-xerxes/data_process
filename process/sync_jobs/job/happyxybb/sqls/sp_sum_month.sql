DROP PROCEDURE IF EXISTS `sp_sum_month`;
CREATE PROCEDURE `sp_sum_month`()
BEGIN
 declare yearmonthid int;
 declare startid int;
 declare endid int;
 declare abnormal_order int;
 declare abnormal_amount int;
 declare cost int;

 set abnormal_order =48;
 set abnormal_amount = 24000;
 set cost = 50;
 
 set yearmonthid = DATE_FORMAT(curdate(),'%Y%m');
 set startid = (SELECT min(dim_date_id) FROM dim_date_day 
     where substr(dim_date_id,1,6)= DATE_FORMAT(date_sub(curdate(),interval 12 month),'%Y%m'));
 set endid = (SELECT max(dim_date_id) FROM dim_date_day 
     where substr(dim_date_id,1,6)= DATE_FORMAT(date_sub(curdate(),interval 1 month),'%Y%m'));



 delete from  sum_month_member_rfm where yearmonth=yearmonthid;

 
 insert into sum_month_member_rfm(dim_member_id,member_no,yearmonth,start_date_id,end_date_id,count_day,count_order,amount,quantity,last_shoping_date_id,last_item_id)
 SELECT a.dim_member_id,b.member_no,yearmonthid as yearmonth, startid as start_date_id,endid as end_date_id,
 count(distinct a.dim_date_id) as count_day,count(distinct a.order_no) as count_order,
 sum(a.amount) as amount,sum(a.quantity) as quantity,max(a.dim_date_id) as last_shoping_date_id, max(item_id) as last_item_id
 FROM fct_sales AS a  
 inner join dim_member b on a.dim_member_id=b.dim_member_id
 where a.dim_date_id between startid and endid 
 group by a.dim_member_id,b.member_no;
 Commit;


 Update sum_month_member_rfm a 
 Inner join 
 (SELECT a.dim_member_id,max(a.dim_date_id) as pre_shoping_date_id
  FROM fct_sales AS a
  INNER JOIN sum_month_member_rfm AS b ON a.dim_member_id = b.dim_member_id and yearmonth=yearmonthid
  where  a.dim_date_id<b.last_shoping_date_id 
  group by a.dim_member_id) b on a.dim_member_id=b.dim_member_id 
 set a.pre_shoping_date_id=b.pre_shoping_date_id 
 where a.yearmonth=yearmonthid ;
 commit;


 update sum_month_member_rfm a set shoping_days =datediff(date(a.last_shoping_date_id),date(ifnull(a.pre_shoping_date_id,a.last_shoping_date_id)))
  where a.yearmonth=yearmonthid;
 commit;


 UPDATE sum_month_member_rfm SET abnormal=1 WHERE   member_no='0' and yearmonth=yearmonthid;
 commit;
 

 UPDATE sum_month_member_rfm SET abnormal=10 where count_day>abnormal_order and abnormal=0 and yearmonth=yearmonthid;
 commit;


 UPDATE sum_month_member_rfm SET abnormal=100 where amount>abnormal_amount and abnormal=0 and yearmonth=yearmonthid;
 commit;

update dim_member a
inner join sum_month_member_rfm b on a.dim_member_id=b.dim_member_id 
set a.abnormal=b.abnormal 
where b.abnormal>0 and b.yearmonth=yearmonthid;
commit;


delete from sum_month_category where yearmonth=yearmonthid;

insert into sum_month_category(yearmonth,categoryI_code,categoryI_name,
sales_total,order_total,sales_amount,order_num,member_num)
select a.yearmonth,a.std_categoryi_code,c.std_category_name,a.sales_total,a.order_total,
  b.sales_amount,b.order_num,b.member_num from
(SELECT
b.yearmonth,a.std_categoryi_code,sum(a.amount) sales_total,count(distinct a.order_no) order_total
FROM
fct_sales AS a
INNER JOIN sum_month_member_rfm AS b ON a.dim_member_id = b.dim_member_id and b.yearmonth=yearmonthid
where a.dim_date_id between b.start_date_id and b.end_date_id 
group by b.yearmonth,a.std_categoryi_code) a
inner join 
(SELECT
b.yearmonth,a.std_categoryi_code,sum(a.amount) sales_amount,count(distinct a.order_no) order_num,count(distinct a.dim_member_id) member_num
FROM
fct_sales AS a
INNER JOIN sum_month_member_rfm AS b ON a.dim_member_id = b.dim_member_id and b.yearmonth=yearmonthid
where a.dim_date_id between b.start_date_id and b.end_date_id 
and b.abnormal=0 
group by b.yearmonth,a.std_categoryi_code) b on a.yearmonth=b.yearmonth and a.std_categoryi_code=b.std_categoryi_code 
inner join v_std_categoryI c on a.std_categoryi_code=c.std_category_code;

commit;




delete from sum_month_member_cost where yearmonth=yearmonthid;

insert into sum_month_member_cost(yearmonth,amount,member_num,proportion)
select a.*, round(a.member_num/b.member_all,4) proportion from 
(SELECT a.yearmonth, ceil(a.amount/cost)*cost AS amount,Count(a.member_no) member_num
FROM
sum_month_member_rfm AS a
where a.abnormal=0 and a.amount>0 and a.yearmonth=yearmonthid
Group by yearmonth,ceil(a.amount/cost)*cost
order by amount) a,
(select Count(a.member_no) member_all FROM
sum_month_member_rfm a 
where a.abnormal=0 and a.amount>0 and a.yearmonth=yearmonthid) b ; 

update sum_month_member_cost a 
Inner join 
(SELECT b.yearmonth,b.amount,sum(a.proportion) sum_proportion
FROM
sum_month_member_cost AS a 
inner join sum_month_member_cost AS b on a.amount<=b.amount 
where a.yearmonth=yearmonthid and b.yearmonth=yearmonthid
group by b.yearmonth,b.amount) b on a.yearmonth=b.yearmonth and a.amount=b.amount 
set a.sum_proportion = b.sum_proportion 
where a.yearmonth=yearmonthid;




delete from sum_month_member_last_interval where yearmonth=yearmonthid;
insert into sum_month_member_last_interval(yearmonth,interval_day,member_num,proportion)
select a.*, round(a.member_num/b.member_all,4) proportion from 
(SELECT
a.yearmonth,a.shoping_days interval_day,count(a.member_no) member_num
FROM
sum_month_member_rfm AS a
where a.abnormal=0 and a.shoping_days>0 and a.yearmonth=yearmonthid
Group by a.yearmonth,a.shoping_days
order by a.shoping_days) a ,
(select Count(a.member_no) member_all FROM
sum_month_member_rfm a 
where a.abnormal=0 and a.shoping_days>0 and a.yearmonth=yearmonthid) b;

update sum_month_member_last_interval a 
Inner join 
(SELECT b.yearmonth,b.interval_day,sum(a.proportion) sum_proportion
FROM
sum_month_member_last_interval AS a 
inner join sum_month_member_last_interval AS b on a.interval_day<=b.interval_day 
where a.yearmonth=yearmonthid and b.yearmonth=yearmonthid
group by b.yearmonth,b.interval_day) b on a.yearmonth=b.yearmonth and a.interval_day=b.interval_day 
set a.sum_proportion = b.sum_proportion 
where a.yearmonth=yearmonthid;




delete from sum_month_member_shopping_times where yearmonth=yearmonthid;
insert into sum_month_member_shopping_times(yearmonth,shopping_times,member_num,proportion)
select a.*, round(a.member_num/b.member_all,4) proportion from 
(SELECT
a.yearmonth,a.count_day shopping_times,Count(a.member_no) member_num
FROM
sum_month_member_rfm AS a
where a.abnormal=0 and a.yearmonth=yearmonthid
Group by a.yearmonth,a.count_day
order by a.count_day) a,
(select Count(a.member_no) member_all FROM
sum_month_member_rfm a 
where a.abnormal=0 and a.yearmonth=yearmonthid) b 
order by a.shopping_times; 

update sum_month_member_shopping_times a 
Inner join 
(SELECT b.yearmonth,b.shopping_times,sum(a.proportion) sum_proportion
FROM
sum_month_member_shopping_times AS a 
inner join sum_month_member_shopping_times AS b on a.shopping_times<=b.shopping_times 
where a.yearmonth=yearmonthid and b.yearmonth=yearmonthid
group by b.yearmonth,b.shopping_times) b on a.yearmonth=b.yearmonth and a.shopping_times=b.shopping_times 
set a.sum_proportion = b.sum_proportion 
where a.yearmonth=yearmonthid;



delete from sum_month_member_mba where yearmonth=yearmonthid;
INSERT INTO sum_month_member_mba(yearmonth, dim_member_id,member_no, c01, c02, c03, c04, c05, c06, c07, c08, c09,c10)
SELECT yearmonthid, dim_member_id,member_no, SUM(c01) AS c01, SUM(c02) AS c02, SUM(c03) AS c03, SUM(c04) AS c04, SUM(c05) AS c05, SUM(c06) AS c06, SUM(c07) AS c07, SUM(c08) AS c08, SUM(c09) AS c09,sum(c10) as c10  FROM
(
SELECT dim_member_id,member_no,
CASE std_categoryi_code WHEN '01' THEN 1 ELSE 0 END AS c01,
CASE std_categoryi_code WHEN '02' THEN 1 ELSE 0 END AS c02,
CASE std_categoryi_code WHEN '03' THEN 1 ELSE 0 END AS c03,
CASE std_categoryi_code WHEN '04' THEN 1 ELSE 0 END AS c04,
CASE std_categoryi_code WHEN '05' THEN 1 ELSE 0 END AS c05,
CASE std_categoryi_code WHEN '06' THEN 1 ELSE 0 END AS c06,
CASE std_categoryi_code WHEN '07' THEN 1 ELSE 0 END AS c07,
CASE std_categoryi_code WHEN '08' THEN 1 ELSE 0 END AS c08,
CASE std_categoryi_code WHEN '09' THEN 1 ELSE 0 END AS c09,
CASE std_categoryi_code WHEN '10' THEN 1 ELSE 0 END AS c10
FROM
(SELECT a.dim_member_id,b.member_no, a.std_categoryi_code FROM fct_sales a 
inner join sum_month_member_rfm b on a.dim_member_id=b.dim_member_id  and b.yearmonth=yearmonthid and b.abnormal=0 
WHERE  a.dim_date_id between b.start_date_id and b.end_date_id GROUP BY a.dim_member_id,b.member_no, a.std_categoryi_code) sx
) sy
GROUP BY
     dim_member_id,member_no;
COMMIT;


delete from sum_month_member where yearmonth=yearmonthid;
insert into sum_month_member(yearmonth,sum_value_name,avg_v)
SELECT yearmonthid, '会员总花费', SUM(amount)/COUNT(member_no) rjxf
FROM sum_month_member_rfm where yearmonth=yearmonthid and abnormal=0 and amount>0;

insert into sum_month_member(yearmonth,sum_value_name,avg_v)
SELECT yearmonthid, '会员购物次数', sum(count_day)/COUNT(member_no) rjcs
FROM sum_month_member_rfm where yearmonth=yearmonthid and abnormal=0 and amount>0;

insert into sum_month_member(yearmonth,sum_value_name,avg_v)
SELECT yearmonthid, '会员单天花费', sum(amount) /sum(count_day) xf_day
FROM sum_month_member_rfm where yearmonth=yearmonthid and abnormal=0 and amount>0;

insert into sum_month_member(yearmonth,sum_value_name,avg_v)
SELECT yearmonthid, '会员单天购买数量', sum(quantity)/sum(count_day) sl_day
FROM sum_month_member_rfm where yearmonth=yearmonthid and abnormal=0 and amount>0;

insert into sum_month_member(yearmonth,sum_value_name,avg_v)
SELECT yearmonthid, '客单价', sum(amount) /sum(count_order) kdj
FROM sum_month_member_rfm where yearmonth=yearmonthid and abnormal=0 and amount>0 ;

insert into sum_month_member(yearmonth,sum_value_name,avg_v)
SELECT yearmonthid,'会员购物周期',SUM(shoping_days)/COUNT(member_no) rjgwzq
FROM sum_month_member_rfm where yearmonth=yearmonthid and abnormal=0 and shoping_days>0;
commit;

update sum_month_member set v25=
(select v25 from 
(SELECT (@rowNO := @rowNo+1) AS rowno, amount v25 FROM sum_month_member_rfm a,(select @rowNO :=0) b where yearmonth=yearmonthid and abnormal=0 and amount>0 order by amount) a
where rowno=round((select count(1) from sum_month_member_rfm where yearmonth=yearmonthid and abnormal=0 and amount>0)/4,0)) 
where yearmonth=yearmonthid and sum_value_name='会员总花费';
update sum_month_member set v50=
(select v50 from 
(SELECT (@rowNO := @rowNo+1) AS rowno, amount v50 FROM sum_month_member_rfm a,(select @rowNO :=0) b where yearmonth=yearmonthid and abnormal=0 and amount>0 order by amount) a
where rowno=round((select count(1) from sum_month_member_rfm where yearmonth=yearmonthid and abnormal=0 and amount>0)/2,0)) 
where yearmonth=yearmonthid and sum_value_name='会员总花费';
update sum_month_member set v75=
(select v75 from 
(SELECT (@rowNO := @rowNo+1) AS rowno, amount v75 FROM sum_month_member_rfm a,(select @rowNO :=0) b where yearmonth=yearmonthid and abnormal=0 and amount>0 order by amount) a
where rowno=round((select count(1) from sum_month_member_rfm where yearmonth=yearmonthid and abnormal=0 and amount>0)/4*3,0)) 
where yearmonth=yearmonthid and sum_value_name='会员总花费';
commit;

update sum_month_member set v25=
(select v25 from 
(SELECT (@rowNO := @rowNo+1) AS rowno, round(amount/count_day,2) v25  FROM sum_month_member_rfm a,(select @rowNO :=0) b where yearmonth=yearmonthid and abnormal=0 and amount>0 order by round(amount/count_day,2) ) a
where rowno=round((select count(1) from sum_month_member_rfm where yearmonth=yearmonthid and abnormal=0 and amount>0)/4,0))
where yearmonth=yearmonthid and sum_value_name='会员单天花费';
update sum_month_member set v50=
(select v50 from 
(SELECT (@rowNO := @rowNo+1) AS rowno, round(amount/count_day,2) v50  FROM sum_month_member_rfm a,(select @rowNO :=0) b where yearmonth=yearmonthid and abnormal=0 and amount>0 order by round(amount/count_day,2) ) a
where rowno=round((select count(1) from sum_month_member_rfm where yearmonth=yearmonthid and abnormal=0 and amount>0)/2,0))
where yearmonth=yearmonthid and sum_value_name='会员单天花费';
update sum_month_member set v75=
(select v75 from 
(SELECT (@rowNO := @rowNo+1) AS rowno, round(amount/count_day,2) v75  FROM sum_month_member_rfm a,(select @rowNO :=0) b where yearmonth=yearmonthid and abnormal=0 and amount>0 order by round(amount/count_day,2) ) a
where rowno=round((select count(1) from sum_month_member_rfm where yearmonth=yearmonthid and abnormal=0 and amount>0)/4*3,0))
where yearmonth=yearmonthid and sum_value_name='会员单天花费';
commit;

update sum_month_member set v25=
(select v25 from 
(SELECT (@rowNO := @rowNo+1) AS rowno, round(quantity/count_day,2) v25  FROM sum_month_member_rfm a,(select @rowNO :=0) b where yearmonth=yearmonthid and abnormal=0 and amount>0 order by round(quantity/count_day,2) ) a
where rowno=round((select count(1) from sum_month_member_rfm where yearmonth=yearmonthid and abnormal=0 and amount>0)/4,0))
where yearmonth=yearmonthid and sum_value_name='会员单天购买数量';
update sum_month_member set v50=
(select v50 from 
(SELECT (@rowNO := @rowNo+1) AS rowno, round(quantity/count_day,2) v50  FROM sum_month_member_rfm a,(select @rowNO :=0) b where yearmonth=yearmonthid and abnormal=0 and amount>0 order by round(quantity/count_day,2) ) a
where rowno=round((select count(1) from sum_month_member_rfm where yearmonth=yearmonthid and abnormal=0 and amount>0)/2,0))
where yearmonth=yearmonthid and sum_value_name='会员单天购买数量';
update sum_month_member set v75=
(select v75 from 
(SELECT (@rowNO := @rowNo+1) AS rowno, round(quantity/count_day,2) v75  FROM sum_month_member_rfm a,(select @rowNO :=0) b where yearmonth=yearmonthid and abnormal=0 and amount>0 order by round(quantity/count_day,2) ) a
where rowno=round((select count(1) from sum_month_member_rfm where yearmonth=yearmonthid and abnormal=0 and amount>0)/4*3,0))
where yearmonth=yearmonthid and sum_value_name='会员单天购买数量';
commit;

update sum_month_member set v25=
(select v25 from 
(SELECT (@rowNO := @rowNo+1) AS rowno, round(amount/count_order,2) v25  FROM sum_month_member_rfm a,(select @rowNO :=0) b where yearmonth=yearmonthid and abnormal=0 and amount>0 order by round(amount/count_order,2) ) a
where rowno=round((select count(1) from sum_month_member_rfm where yearmonth=yearmonthid and abnormal=0 and amount>0)/4,0))
where yearmonth=yearmonthid and sum_value_name='客单价';
update sum_month_member set v50=
(select v50 from 
(SELECT (@rowNO := @rowNo+1) AS rowno, round(amount/count_order,2) v50  FROM sum_month_member_rfm a,(select @rowNO :=0) b where yearmonth=yearmonthid and abnormal=0 and amount>0 order by round(amount/count_order,2) ) a
where rowno=round((select count(1) from sum_month_member_rfm where yearmonth=yearmonthid and abnormal=0 and amount>0)/2,0))
where yearmonth=yearmonthid and sum_value_name='客单价';
update sum_month_member set v75=
(select v75 from 
(SELECT (@rowNO := @rowNo+1) AS rowno, round(amount/count_order,2) v75  FROM sum_month_member_rfm a,(select @rowNO :=0) b where yearmonth=yearmonthid and abnormal=0 and amount>0 order by round(amount/count_order,2) ) a
where rowno=round((select count(1) from sum_month_member_rfm where yearmonth=yearmonthid and abnormal=0 and amount>0)/4*3,0))
where yearmonth=yearmonthid and sum_value_name='客单价';
commit;

update sum_month_member set v25=
(select min(shopping_times) from sum_month_member_shopping_times where yearmonth=yearmonthid and sum_proportion>0.25) 
where yearmonth=yearmonthid and sum_value_name='会员购物次数';
update sum_month_member set v50=
(select min(shopping_times) from sum_month_member_shopping_times where yearmonth=yearmonthid and sum_proportion>0.5) 
where yearmonth=yearmonthid and sum_value_name='会员购物次数';
update sum_month_member set v75=
(select min(shopping_times) from sum_month_member_shopping_times where yearmonth=yearmonthid and sum_proportion>0.75) 
where yearmonth=yearmonthid and sum_value_name='会员购物次数';
commit;

update sum_month_member set v25=
(select min(interval_day) from sum_month_member_last_interval where yearmonth=yearmonthid and sum_proportion>0.25) 
where yearmonth=yearmonthid and sum_value_name='会员购物周期';
update sum_month_member set v50=
(select min(interval_day) from sum_month_member_last_interval where yearmonth=yearmonthid and sum_proportion>0.5) 
where yearmonth=yearmonthid and sum_value_name='会员购物周期';
update sum_month_member set v75=
(select min(interval_day) from sum_month_member_last_interval where yearmonth=yearmonthid and sum_proportion>0.75) 
where yearmonth=yearmonthid and sum_value_name='会员购物周期';
commit;

delete from sum_month_total where yearmonth=yearmonthid;
insert into sum_month_total
select yearmonth,'销售总额',sum(amount) ,sum(case dim_member_id when 0 then amount else 0 end ),  
sum(case dim_member_id when 0 then 0 else amount end ), 
sum(case when dim_member_id>0 and abnormal=0 then amount else 0 end )
from sum_month_member_rfm where yearmonth=yearmonthid 
group by yearmonth;
insert into sum_month_total
select yearmonth,'小票数',sum(count_order) ,sum(case dim_member_id when 0 then count_order else 0 end ),  
sum(case dim_member_id when 0 then 0 else count_order end ), 
sum(case when dim_member_id>0 and abnormal=0 then count_order else 0 end )
from sum_month_member_rfm where yearmonth=yearmonthid
group by yearmonth;
insert into sum_month_total
select yearmonth,'会员数',count(dim_member_id) ,sum(case dim_member_id when 0 then 1 else 0 end ),  
sum(case dim_member_id when 0 then 0 else 1 end ), 
sum(case when dim_member_id>0 and abnormal=0 then 1 else 0 end )
from sum_month_member_rfm where yearmonth=yearmonthid
group by yearmonth;

commit;


delete from sum_month_category_ageperiod where yearmonth=yearmonthid;
insert into sum_month_category_ageperiod(yearmonth,categoryI_code,categoryI_name,age_period,age_period_name,
categoryI_total,categoryI_quantity,order_total,order_quantity,order_num,order_date_num,member_num)
select a.yearmonth,a.std_categoryi_code,c.std_category_name,d.agegroupii_code,d.agegroupii_name,
a.categoryI_total,a.categoryI_quantity,b.order_total,b.order_quantity,a.order_num,a.order_date_num,a.member_num from
(SELECT
b.yearmonth,a.std_categoryi_code,a.dim_agegroup_id,sum(a.amount) categoryI_total,sum(a.quantity) categoryI_quantity,
count(distinct a.order_no) order_num, count(distinct  a.dim_member_id,a.dim_date_id) order_date_num, count(distinct a.dim_member_id) member_num
FROM
fct_sales AS a
INNER JOIN sum_month_member_rfm AS b ON a.dim_member_id = b.dim_member_id and b.yearmonth=yearmonthid
where a.dim_date_id between b.start_date_id and b.end_date_id 
and a.dim_agegroup_id between 1 and 9
group by b.yearmonth,a.std_categoryi_code,a.dim_agegroup_id) a
inner join 
(select a.yearmonth,a.std_categoryi_code,a.dim_agegroup_id, sum(b.order_total) order_total, sum(b.order_quantity) order_quantity 
from
(SELECT
b.yearmonth,a.std_categoryi_code,a.dim_agegroup_id,a.order_no
FROM
fct_sales AS a
INNER JOIN sum_month_member_rfm AS b ON a.dim_member_id = b.dim_member_id and b.yearmonth=yearmonthid
where a.dim_date_id between b.start_date_id and b.end_date_id  
and a.dim_agegroup_id between 1 and 9
group by b.yearmonth,a.std_categoryi_code,a.dim_agegroup_id,a.order_no) a
inner join 
(SELECT
a.order_no,sum(a.amount) order_total,sum(a.quantity) order_quantity
FROM
fct_sales AS a
INNER JOIN sum_month_member_rfm AS b ON a.dim_member_id = b.dim_member_id and b.yearmonth=yearmonthid
where a.dim_date_id between b.start_date_id and b.end_date_id  
and a.dim_agegroup_id between 1 and 9
group by a.order_no) b On a.order_no=b.order_no 
group by a.yearmonth,a.std_categoryi_code,a.dim_agegroup_id) b 
on a.yearmonth=b.yearmonth and a.std_categoryi_code=b.std_categoryi_code and a.dim_agegroup_id=b.dim_agegroup_id
inner join v_std_categoryI c on a.std_categoryi_code=c.std_category_code 
inner join dim_agegroup d on a.dim_agegroup_id=d.dim_agegroup_id;


update sum_month_category_ageperiod set avg_order_cost=round(order_total/order_num,2) where yearmonth=yearmonthid;
update sum_month_category_ageperiod set avg_order_date_cost=round(order_total/order_date_num,2) where yearmonth=yearmonthid;
commit;

END