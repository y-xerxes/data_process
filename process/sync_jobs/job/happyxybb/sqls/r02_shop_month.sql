DROP PROCEDURE IF EXISTS `r02_shop_month`;
CREATE PROCEDURE `r02_shop_month`()
BEGIN
 SET @end_d  = DATE_FORMAT(start_date,'%Y%m%d');
 set @start_d = (select dim_date_id  from Dim_Date_Day a Inner join 
                   (select date_year_code as date_year_code, date_month_code from Dim_Date_Day where dim_date_id=@end_d) b 
                   on a.date_year_code=b.date_year_code and  a.date_month_code=b.date_month_code and a.date_day_code=1);
 SET @end_t = (select dim_date_id  from Dim_Date_Day a Inner join 
                   (select date_year_code-1 as date_year_code, date_month_code, date_day_code from Dim_Date_Day where dim_date_id=@end_d) b 
                   on a.date_year_code=b.date_year_code and  a.date_month_code=b.date_month_code and a.date_day_code=b.date_day_code);
 set @start_t = (select dim_date_id  from Dim_Date_Day a Inner join 
                   (select date_year_code-1 as date_year_code, date_month_code from Dim_Date_Day where dim_date_id=@end_d) b 
                   on a.date_year_code=b.date_year_code and  a.date_month_code=b.date_month_code and a.date_day_code=1);
 

 Select x.time_type, x.type_name, x.shop_count,
	ifnull(n_sum_amount,0) as n_sum_amount, 
	ifnull(t_sum_amount,0) as t_sum_amount, 
	ifnull((n_sum_amount-t_sum_amount)/ABS(t_sum_amount),0) as amount_z,
  ifnull(n_sum_quantity,0) as n_sum_quantity, 
	ifnull(t_sum_quantity,0) as t_sum_quantity, 
	ifnull((n_sum_quantity-t_sum_quantity)/ABS(t_sum_quantity),0) as quantity_z,
	ifnull(n_order_num,0) as n_order_num, 
	ifnull(t_order_num,0) as t_order_num,
	ifnull((n_order_num-t_order_num)/ABS(t_order_num),0) as order_z, 

	ifnull(n_sum_amount/ABS(n_order_num),0) as n_order_price,
	ifnull(t_sum_amount/(t_order_num),0) as t_order_price,
	ifnull(((n_sum_amount/ABS(n_order_num))-(t_sum_amount/ABS(t_order_num)))/ABS((t_sum_amount/t_order_num)),0) as order_price_z 
from(
	select '月' as time_type,
			d.shopii_name as type_name,
			count(distinct d.shop_code) as shop_count,
			sum(amount) as n_sum_amount,
			sum(quantity) as n_sum_quantity, 
			sum(count_order) as n_order_num
		from sum_day_sales_shop as s,  dim_shop d
		where s.dim_shop_id = d.dim_shop_id
			and s.dim_date_id between  @start_d and @end_d
			and d.shopii_name is not null
		group by d.shopii_name
	)as x  left join
(
		select '月' as time_type,
			d.shopii_name as type_name,
			sum(amount) as t_sum_amount, 
      sum(quantity) as t_sum_quantity, 
			sum(count_order) as t_order_num
		from sum_day_sales_shop as s,  dim_shop d
		where s.dim_shop_id = d.dim_shop_id
			and s.dim_date_id between  @start_t and @end_t
			and d.shopii_name is not null
		group by d.shopii_name
)as y
on x.type_name = y.type_name;

END