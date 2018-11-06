DROP PROCEDURE IF EXISTS `r01_list`;
CREATE PROCEDURE `r01_list`(IN start_date varchar(10), IN typeid INT)
BEGIN
 DECLARE stm VARCHAR(20000);
 SET @end_d  = DATE_FORMAT(start_date,'%Y%m%d'); 

 If (typeid=1) then
    set @start_d = DATE_FORMAT(date_sub(start_date,interval 6 day),'%Y%m%d');
    set @str1 = 'b.dim_date_id';
  ELSEIF (typeid=2) then
    set @start_d = (select dim_date_id  from dim_date_day a Inner join 
                   (select date_yearweek_code as date_yearweek_code from dim_date_day where dim_date_id=DATE_FORMAT(date_sub(start_date,interval 3 week),'%Y%m%d')) b 
                   on a.date_yearweek_code=b.date_yearweek_code and a.date_weekday_code=1);
    set @str1 = 'b.date_yearweek_code';
  ELSEIF (typeid=3) then
    set @start_d = (select dim_date_id  from dim_date_day a Inner join 
                   (select date_year_code as date_year_code, date_month_code from dim_date_day where dim_date_id=DATE_FORMAT(date_sub(start_date,interval 2 month),'%Y%m%d')) b 
                   on a.date_year_code=b.date_year_code and  a.date_month_code=b.date_month_code and a.date_day_code=1);
    set @str1 = 'substr(b.dim_date_id,1,6)';
  END IF;

SET @sql1=CONCAT('
SELECT d.id, case ',typeid,' when 1 then ''日'' when 2 then ''周'' when 3 then ''月'' end id_name,

d.amount,d.cn_member,d.cn_order,case d.cn_order when 0 then 0 else d.amount/d.cn_order end kprice,
d1.amount_1,d1.cn_member_1,d1.cn_order_1,case d1.cn_order_1 when 0 then 0 else d1.amount_1/d1.cn_order_1 end kprice_1,
d2.amount_2,d2.cn_member_2,d2.cn_order_2,case d2.cn_order_2 when 0 then 0 else d2.amount_2/d2.cn_order_2 end kprice_2,
d3.amount_3,d3.cn_member_3,d3.cn_order_3,case d3.cn_order_3 when 0 then 0 else d3.amount_3/d3.cn_order_3 end kprice_3

from 
(SELECT ',@str1,' as id,sum(a.amount) as amount,count(distinct a.dim_member_id) as cn_member,count(distinct a.order_no) cn_order
 FROM fct_sales AS a
 inner join dim_date_day b on a.dim_date_id= b.dim_date_id
 WHERE a.Dim_Date_ID between @start_d and  @end_d 
 group by ',@str1,') d 
left join 
(SELECT ',@str1,' as id,sum(a.amount) as amount_1,1 as cn_member_1,count(distinct a.order_no) cn_order_1
 FROM fct_sales AS a
 inner join dim_date_day b on a.dim_date_id= b.dim_date_id
 WHERE a.Dim_Date_ID between @start_d and  @end_d and a.dim_member_id = 0 
 group by ',@str1,'
) AS d1 on d.id=d1.id
left join 
(SELECT ',@str1,' as id,sum(a.amount) as amount_2,count(distinct a.dim_member_id) as cn_member_2,count(distinct a.order_no) cn_order_2
 FROM fct_sales AS a
 inner join dim_date_day b on a.dim_date_id= b.dim_date_id
 WHERE a.Dim_Date_ID between @start_d and  @end_d and a.dim_member_id > 0  and ifnull(dim_pre_date_id,0)=0
group by ',@str1,'
) AS d2 on d.id=d2.id
left join 
(SELECT ',@str1,' as id,sum(a.amount) as amount_3,count(distinct a.dim_member_id) as cn_member_3,count(distinct a.order_no) cn_order_3
 FROM fct_sales AS a
 inner join dim_date_day b on a.dim_date_id= b.dim_date_id
 WHERE a.Dim_Date_ID between @start_d and  @end_d and a.dim_member_id > 0  and dim_pre_date_id>0
group by ',@str1,'
) AS d3 on d.id=d3.id') ;
 
PREPARE stmt FROM @sql1;  EXECUTE stmt;  DEALLOCATE PREPARE stmt;  
  



END