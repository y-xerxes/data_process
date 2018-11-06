DROP PROCEDURE IF EXISTS `sp_submit_fct`;
CREATE PROCEDURE `sp_submit_fct`()
begin
   declare not_found_flag  int default 0;
   declare dateid int;
   declare import_id int;
   
   declare cur_date cursor for 
         select dim_date_id from tmp_sales_item group by dim_date_id order by dim_date_id;
   declare continue handler for not found set not_found_flag  = 1;
   insert into event_log(event_name,event_type,event_date) values('submit_fct','start',now());  
   select event_type into import_id from event_log where event_name='importdata' and date_format(event_date,'%Y%m%d')=date_format(curdate(),'%Y%m%d');
   if import_id = 2 then 
   open cur_date;
   repeat 
   fetch  cur_date into dateid; 

   if not  not_found_flag then
   begin
       delete a.* from  tmp_sales_item a
       inner join (select dim_shop_id,dim_date_id,order_no from fct_sales where  dim_date_id=dateid group by dim_shop_id,dim_date_id,order_no) b
           on a.dim_shop_id=b.dim_shop_id and a.dim_date_id=b.dim_date_id and a.order_no=b.order_no
       where a.dim_date_id=dateid ;
       commit;

       update tmp_sales_item a 
       inner join 
       (select dim_member_id,max(dim_date_id) as pre_date_id
           from fct_sales 
           where  dim_date_id<dateid and dim_member_id>0
           group by dim_member_id) b on a.dim_member_id=b.dim_member_id  
       set a.dim_pre_date_id=b.pre_date_id
       where a.dim_date_id=dateid;
       commit;

       Insert into fct_sales
             (sales_time,dim_product_id,dim_date_id,dim_shop_id,dim_agegroup_id,dim_member_id,dim_pre_date_id,age_days,
           order_no,item_seq,sales_price,fact_price,quantity,amount,profit,flag,
           dim_categoryI_id,dim_categoryII_id,dim_categoryIII_id,dim_brand_id,std_categoryi_code,guider_code)
       select a.sales_time,a.dim_product_id,a.dim_date_id,a.dim_shop_id,a.dim_agegroup_id,a.dim_member_id,a.dim_pre_date_id,a.age_days,
           a.order_no,a.item_seq,a.sales_price,a.fact_price,a.quantity,a.amount,a.profit,a.flag,
           a.dim_categoryI_id,a.dim_categoryII_id,a.dim_categoryIII_id,a.dim_brand_id,a.std_categoryi_code,a.guider_code
       from tmp_sales_item a
       where a.dim_date_id=dateid ;
       commit;

       insert into his_sales_item(order_no,item_seq,sales_time,shop_code,member_code,member_no,product_code,sales_price,
           fact_price,quantity,amount,profit,o_categoryI,categoryI,dim_date_id,guider_code)
       SELECT a.order_no,a.item_seq,a.sales_time,a.shop_code,a.member_code,a.member_no,a.product_code,a.sales_price,
           a.fact_price,a.quantity,a.amount,a.profit,b.categoryi_code,b.std_categoryi_code,a.dim_date_id,a.guider_code
       from tmp_sales_item a
       left JOIN dim_product AS b ON a.product_code = b.product_code     
       where a.dim_date_id=dateid ;
       commit;

       Insert into fct_billing
             (order_no,sales_time,dim_date_id,dim_member_id,member_code,member_no,
              dim_shop_id,shop_code,due_amount,guider_code,nrp_amount,real_amount,discount_amount,updated_at)
       select a.*,ifnull(b.real_amount,0),a.due_amount-ifnull(b.real_amount,0) discount_amount,now() from (
       select a.order_no,min(a.sales_time) sales_time,a.dim_date_id,a.dim_member_id,a.member_code,a.member_no,
              a.dim_shop_id,a.shop_code,sum(a.amount) due_amount,a.guider_code, 
              sum(case when a.std_categoryi_code='93' then a.amount else 0 end ) nrp_amount 
       from tmp_sales_item a 
       where a.dim_date_id=dateid 
       group by a.order_no) a 
       left join tmp_billing b on a.order_no=b.order_no;
       commit;

       Insert into fct_billing_coupon
             (order_no,billing_time,shop_code,coupon_code,dim_date_id,amount,updated_at)
       select a.order_no,a.billing_time,a.shop_code,a.coupon_code,a.dim_date_id,a.amount,now() from tmp_billing_coupon a
       inner join ( 
       select a.order_no  
       from tmp_sales_item a 
       where a.dim_date_id=dateid 
       group by a.order_no) b on a.order_no=b.order_no;
       commit;

       update dim_member a 
       inner join (select dim_member_id, min(sales_time) sales_time from fct_sales
           where dim_member_id>0 and dim_date_id=dateid group  by dim_member_id) b On a.dim_member_id=b.dim_member_id
       set a.first_shoping=b.sales_time
       where a.first_shoping is null;
       commit;

       update dim_member a 
       inner join (select distinct a.dim_member_id,  a.sales_time , b.shop_code from fct_sales a
       inner join dim_shop b on a.dim_shop_id=b.dim_shop_id
           where a.dim_member_id>0 and a.dim_date_id=dateid group  by a.dim_member_id order by a.sales_time desc ) b on a.dim_member_id=b.dim_member_id
       set a.last_shoping=b.sales_time,a.last_shoping_code=b.shop_code
       where a.last_shoping is null or a.last_shoping<b.sales_time;
       commit;

       update dim_member
       set reg_shop_code=last_shoping_code 
       where ifnull(reg_shop_code,'')='' and ifnull(last_shoping_code,'')<>''; 
       commit;

   end;
   end if;
   until not_found_flag end repeat; 
   close cur_date;

   update dim_member a 
   inner join
   (SELECT a.dim_member_id,max(b.guider_code) guider_code FROM dim_member a 
      inner join fct_sales b on a.dim_member_id=b.dim_member_id and date_format(a.first_shoping,'%Y%m%d')=b.dim_date_id 
      where a.first_shoping is not null 
      group by a.dim_member_id) b on a.dim_member_id=b.dim_member_id 
   set a.fo_guider_code=ifnull(b.guider_code,'0') 
   where a.fo_guider_code is null;
   commit;
   
   update dim_member a
   inner join (select dim_member_id,min(dim_date_id) first_shoping_online,max(dim_date_id) last_shoping_online from fct_sales 
   where flag=1 
   group by dim_member_id) b on a.dim_member_id=b.dim_member_id 
   set a.first_shoping_online=b.first_shoping_online , a.last_shoping_online=b.last_shoping_online 
   where a.app_flag=1;
   commit;
   insert into event_log(event_name,event_type,event_date) values('submit_fct','end',now());
   end if;
end