DROP PROCEDURE IF EXISTS `sp_deal_fct`;
CREATE PROCEDURE `sp_deal_fct`()
begin
   declare import_id int;
   declare date_id int; 
   select event_type into import_id from event_log where event_name='importdata' and date_format(event_date,'%Y%m%d')=date_format(curdate(),'%Y%m%d');
   if import_id = 2 then
       insert into event_log(event_name,event_type,event_date) values('deal_fct','start',now());
       
       delete from tmp_sales_item where ifnull(order_no,'')='';
       update tmp_sales_item set order_no=trim(order_no),product_code=trim(product_code),member_code=trim(member_code),guider_code=trim(guider_code);

       update tmp_sales_item a
        set a.member_code='0' ,a.member_no='0' where ifnull(member_code,'')='';
       commit;

       update tmp_sales_item a
        set a.dim_date_id=date_format(a.sales_time,'%Y%m%d');
       commit;

       update tmp_sales_item a
        set a.fact_price=a.amount/a.quantity where ifnull(a.fact_price,0)=0;
       commit;

      update tmp_sales_item a
        inner join dim_product b on a.product_code = b.product_code 
        left join dim_product_category c on b.categoryI_code=c.category_code and c.category_type=1
        left join dim_product_category d on b.categoryII_code=d.category_code and d.category_type=2
        left join dim_product_category e on b.categoryIII_code=e.category_code and e.category_type=3
        left join dim_product_brand f on b.brand_code=f.brand_code
        set  a.dim_product_id=b.dim_product_id, a.dim_categoryI_id=c.dim_category_id,a.dim_categoryII_id=d.dim_category_id,
        a.dim_brand_id=f.dim_brand_id,a.dim_categoryIII_id=e.dim_category_id , a.std_categoryi_code=b.std_categoryi_code;
        commit;



       
        

        update tmp_sales_item a
        inner join  dim_shop b on a.shop_code = b.shop_code 
        set  a.dim_shop_id=b.dim_shop_id;
        commit;
  
       
        update tmp_sales_item a
        inner join  dim_member b on a.member_code = b.member_code 
        set  a.dim_member_id=b.dim_member_id, a.member_no=b.member_no,a.age_days= datediff(a.sales_time,b.baby_birthday) 
        where a.member_code<>'0';
        commit;

       
        update tmp_sales_item a
        set  a.dim_member_id = 0, a.age_days= 0, a.dim_agegroup_id= 14 
        where  ifnull(a.dim_member_id,'0')='0';
        commit;

        update tmp_sales_item a set a.age_days=9999 where a.age_days is null;
        
        commit;

       
       update tmp_sales_item a ,  dim_agegroup b
        set a.dim_agegroup_id =b.dim_agegroup_id
        where a.age_days between b.agegroup_minage and b.agegroup_maxage 
        and a.dim_member_id>0 and b.dim_agegroup_id!=14;
        commit; 

       
      insert into event_log(event_name,event_type,event_date) values('deal_fct','end',now());
      
   end if;
     
       
end