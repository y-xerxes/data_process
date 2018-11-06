DROP PROCEDURE IF EXISTS `sp_sum_day`;
CREATE PROCEDURE `sp_sum_day`()
begin
	declare v_start int; 
  declare v_end int;

  declare import_id int;
  
   select event_type into import_id from event_log where event_name='importdata' and date_format(event_date,'%Y%m%d')=date_format(curdate(),'%Y%m%d');
   if import_id = 2 then 

   insert into event_log(event_name,event_type,event_date) values('sum_day','start',now());  
        
        

    
        select dim_date_id into v_start from  dim_date_day where date_code=date_sub(curdate(),interval 7 day);
        select dim_date_id into v_end   from  dim_date_day where date_code=date_sub(curdate(),interval 1 day) ; 

        delete from sum_day_sales_brand where dim_date_id between v_start and v_end;

        insert into sum_day_sales_brand(dim_date_id,dim_shop_id,brand_code,brand_name,amount,count_order,quantity,profit) 
        select a.dim_date_id,a.dim_shop_id,b.brand_code,b.brand_name,
             sum(a.amount) amount,     count(distinct a.order_no) count_order ,sum(quantity) quantity ,sum(profit) profit
        from fct_sales a 
        inner join dim_product_brand  b on a.dim_brand_id=b.dim_brand_id 
        where a.dim_date_id between v_start and v_end
        group by a.dim_date_id,a.dim_shop_id,b.brand_code,b.brand_name  ; 
        commit;

       delete from sum_day_sales_categoryiii where dim_date_id between v_start and v_end;

        insert into sum_day_sales_categoryiii(dim_date_id,dim_shop_id,categoryiii_code,categoryiii_name,amount,count_order,quantity,profit) 
        select a.dim_date_id,a.dim_shop_id,b.category_code,b.category_name,
             sum(a.amount) amount, count(distinct a.order_no) count_order ,sum(quantity) quantity,sum(profit) profit
        from fct_sales a 
        inner join dim_product_category b on a.dim_categoryiii_id=b.dim_category_id and b.category_type=3
        where a.dim_date_id between v_start and v_end
        group by a.dim_date_id,a.dim_shop_id,b.category_code,b.category_name  ; 
        commit;

       delete from sum_day_sales_categoryii where dim_date_id between v_start and v_end;

        insert into sum_day_sales_categoryii(dim_date_id,dim_shop_id,categoryii_code,categoryii_name,amount,count_order,quantity,profit) 
        select a.dim_date_id,a.dim_shop_id,b.category_code,b.category_name,
             sum(a.amount) amount, count(distinct a.order_no) count_order,sum(quantity) quantity ,sum(profit) profit
        from fct_sales a 
        inner join dim_product_category b on a.dim_categoryii_id=b.dim_category_id and b.category_type=2
        where a.dim_date_id between v_start and v_end
        group by a.dim_date_id,a.dim_shop_id,b.category_code,b.category_name  ; 
        commit;
       
        delete from sum_day_sales_categoryi where dim_date_id between v_start and v_end;

        insert into sum_day_sales_categoryi(dim_date_id,dim_shop_id,categoryi_code,categoryi_name,amount,count_order,quantity,profit)
        select a.dim_date_id,a.dim_shop_id,b.category_code,b.category_name,
             sum(a.amount) amount,  count(distinct a.order_no) count_order,sum(quantity) quantity,sum(profit) profit
        from fct_sales a 
        inner join dim_product_category b on a.dim_categoryi_id=b.dim_category_id and b.category_type=1
        where a.dim_date_id between v_start and v_end 
        group by a.dim_date_id,a.dim_shop_id,b.category_code,b.category_name  ; 
      commit;

      delete from  sum_day_sales_shop where dim_date_id between v_start and v_end;

      insert into sum_day_sales_shop(dim_date_id,dim_shop_id,amount,quantity,count_order,profit) 
      select dim_date_id,dim_shop_id, sum(amount) amount,  sum(quantity) quantity,count(distinct order_no) count_order,sum(profit) profit 
      from fct_sales 
      where   dim_date_id between v_start and v_end 
      group by dim_date_id,dim_shop_id ;

      commit;
      

    insert into event_log(event_name,event_type,event_date) values('sum_day','end',now()); 

  end if;

end