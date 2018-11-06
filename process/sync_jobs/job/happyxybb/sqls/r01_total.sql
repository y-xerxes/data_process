DROP PROCEDURE IF EXISTS `r01_total`;
CREATE PROCEDURE `r01_total`(IN start_date varchar(10), IN typeid INT)
BEGIN

 SET @end_d  = DATE_FORMAT(start_date,'%Y%m%d'); 

 If (typeid=1) then
    set @start_d = @end_d;
    SET @end_t = (select dim_date_id  from dim_date_day a Inner join 
                   (select date_yearweek_code-100 as date_yearweek_code, date_weekday_code from dim_date_day where dim_date_id=@end_d) b 
                   on a.date_yearweek_code=b.date_yearweek_code and a.date_weekday_code=b.date_weekday_code);
    set @start_t = @end_t;
  ELSEIF (typeid=2) then
    set @start_d = (select dim_date_id  from dim_date_day a Inner join 
                   (select date_yearweek_code as date_yearweek_code from dim_date_day where dim_date_id=@end_d) b 
                   on a.date_yearweek_code=b.date_yearweek_code and a.date_weekday_code=1);
    SET @end_t = (select dim_date_id  from dim_date_day a Inner join 
                   (select date_yearweek_code-100 as date_yearweek_code, date_weekday_code from dim_date_day where dim_date_id=@end_d) b 
                   on a.date_yearweek_code=b.date_yearweek_code and a.date_weekday_code=b.date_weekday_code);
    set @start_t = (select dim_date_id  from dim_date_day a Inner join 
                   (select date_yearweek_code-100 as date_yearweek_code from dim_date_day where dim_date_id=@end_d) b 
                   on a.date_yearweek_code=b.date_yearweek_code and a.date_weekday_code=1);
  ELSEIF (typeid=3) then
    set @start_d = (select dim_date_id  from dim_date_day a Inner join 
                   (select date_year_code as date_year_code, date_month_code from dim_date_day where dim_date_id=@end_d) b 
                   on a.date_year_code=b.date_year_code and  a.date_month_code=b.date_month_code and a.date_day_code=1);
    SET @end_t = (select dim_date_id  from dim_date_day a Inner join 
                   (select date_year_code-1 as date_year_code, date_month_code, date_day_code from dim_date_day where dim_date_id=@end_d) b 
                   on a.date_year_code=b.date_year_code and  a.date_month_code=b.date_month_code and a.date_day_code=b.date_day_code);
    set @start_t = (select dim_date_id  from dim_date_day a Inner join 
                   (select date_year_code-1 as date_year_code, date_month_code from dim_date_day where dim_date_id=@end_d) b 
                   on a.date_year_code=b.date_year_code and  a.date_month_code=b.date_month_code and a.date_day_code=1);
   END IF;

select d.id,case d.id when 1 then '日' when 2 then '周' when 3 then '月' end id_name,
d.amount,d.cn_member,d.cn_order,case d.cn_order when 0 then 0 else d.amount/d.cn_order end kprice,
d1.amount_1,d1.cn_member_1,d1.cn_order_1,case d1.cn_order_1 when 0 then 0 else d1.amount_1/d1.cn_order_1 end kprice_1,
d2.amount_2,d2.cn_member_2,d2.cn_order_2,case d2.cn_order_2 when 0 then 0 else d2.amount_2/d2.cn_order_2 end kprice_2,
d3.amount_3,d3.cn_member_3,d3.cn_order_3,case d3.cn_order_3 when 0 then 0 else d3.amount_3/d3.cn_order_3 end kprice_3,
t1.amount_1_t,t1.cn_member_1_t,t1.cn_order_1_t,case t1.cn_order_1_t when 0 then 0 else t1.amount_1_t/t1.cn_order_1_t end kprice_1_t,
t2.amount_2_t,t2.cn_member_2_t,t2.cn_order_2_t,case t2.cn_order_2_t when 0 then 0 else t2.amount_2_t/t2.cn_order_2_t end kprice_2_t,
t3.amount_3_t,t3.cn_member_3_t,t3.cn_order_3_t,case t3.cn_order_3_t when 0 then 0 else t3.amount_3_t/t3.cn_order_3_t end kprice_3_t
from 
(SELECT typeid as id,sum(a.amount) as amount,count(distinct a.dim_member_id) as cn_member,count(distinct a.order_no) cn_order
 FROM fct_sales AS a
 WHERE a.Dim_Date_ID between @start_d and @end_d) d 
left join 
(SELECT typeid as id,sum(a.amount) as amount_1,1 as cn_member_1,count(distinct a.order_no) cn_order_1
 FROM fct_sales AS a
 WHERE a.Dim_Date_ID between @start_d and @end_d and a.dim_member_id = 0 
) AS d1 on d.id=d1.id
left join 
(SELECT typeid as id,sum(a.amount) as amount_2,count(distinct a.dim_member_id) as cn_member_2,count(distinct a.order_no) cn_order_2
 FROM fct_sales AS a
 WHERE a.Dim_Date_ID between @start_d and @end_d and a.dim_member_id > 0  and ifnull(dim_pre_date_id,0)=0
) AS d2 on d.id=d2.id
left join 
(SELECT typeid as id,sum(a.amount) as amount_3,count(distinct a.dim_member_id) as cn_member_3,count(distinct a.order_no) cn_order_3
 FROM fct_sales AS a
 WHERE a.Dim_Date_ID between @start_d and @end_d and a.dim_member_id > 0  and dim_pre_date_id>0
) AS d3 on d.id=d3.id 
left join 
(SELECT typeid as id,sum(a.amount) as amount_1_t,1 as cn_member_1_t,count(distinct a.order_no) cn_order_1_t
 FROM fct_sales AS a
 WHERE a.Dim_Date_ID between @start_t and @end_t and a.dim_member_id = 0 
) AS t1 on d.id=t1.id
left join 
(SELECT typeid as id,sum(a.amount) as amount_2_t,count(distinct a.dim_member_id) as cn_member_2_t,count(distinct a.order_no) cn_order_2_t
 FROM fct_sales AS a
 WHERE a.Dim_Date_ID between @start_t and @end_t and a.dim_member_id > 0  and ifnull(dim_pre_date_id,0)=0
) AS t2 on d.id=t2.id
left join 
(SELECT typeid as id,sum(a.amount) as amount_3_t,count(distinct a.dim_member_id) as cn_member_3_t,count(distinct a.order_no) cn_order_3_t
 FROM fct_sales AS a
 WHERE a.Dim_Date_ID between @start_t and @end_t and a.dim_member_id > 0  and dim_pre_date_id>0
) AS t3 on d.id=t3.id ;
  



END