DROP PROCEDURE IF EXISTS `sp_deal_dim`;
CREATE PROCEDURE `sp_deal_dim`()
begin
   declare import_id int;
   select event_type into import_id from event_log where event_name='importdata' and date_format(event_date,'%Y%m%d')=date_format(curdate(),'%Y%m%d');
   if import_id = 1 then
       insert into event_log(event_name,event_type,event_date) values('deal_dim','start',now());

        update tmp_shop set shop_code=trim(shop_code),shop_name=trim(shop_name);
        update tmp_shop a inner join dim_shop b on a.shopII_code=b.shop_code and b.shop_degree=2 
        set a.shopII_name=b.shop_name 
        where a.shopII_name is null;

        update dim_shop a 
        inner join tmp_shop  b on a.shop_code = b.shop_code 
        set a.shop_name=trim(b.shop_name), a.shop_area=b.shop_area,           
            a.shop_open_date=b.shop_open_date,a.shop_close_date=b.shop_close_date,a.updated_at=now();
        commit;

        insert into dim_shop(shop_code,shop_name,shop_area,
            shop_degree,shop_open_date,shop_close_date,shopI_code,shopI_name,shopII_code,shopII_name)
        select trim(a.shop_code),trim(a.shop_name),a.shop_area,
            a.shop_degree,a.shop_open_date,a.shop_close_date,
            '0','总部快乐宝贝','1','自营店'
        from tmp_shop as a
        left join dim_shop  as b on a.shop_code = b.shop_code 
        where b.shop_code is null;
        commit;

       update tmp_product set product_code=trim(product_code),barcode=trim(barcode),
            categoryI_code=trim(categoryI_code),categoryII_code=trim(categoryII_code),
            categoryIII_code=trim(categoryIII_code),brand_code=trim(brand_code); 
       update tmp_product set categoryI_code='99',categoryI_name='其他大类' ,
            categoryII_code='9999',categoryII_name='其他中类',categoryIII_code='999999',categoryIII_name='其他小类'
          where ifnull(categoryI_name,'')='' or ifnull(categoryIii_name,'')='';
       update tmp_product set brand_code='999999',brand_name='其他品牌' where ifnull(brand_name,'')='';

       update dim_product a 
       inner join tmp_product b on a.product_code = b.product_code 
       set a.barcode=replace(b.barcode,'\n',''),a.product_name=replace(b.product_name,'\n',''),
            a.categoryI_code=b.categoryI_code,a.categoryI_name=b.categoryI_name,
            a.categoryII_code=b.categoryII_code,a.categoryII_name=b.categoryII_name,
            a.categoryIII_code=b.categoryIII_code,a.categoryIII_name=b.categoryIII_name,
            a.brand_code=b.brand_code,a.brand_name=b.brand_name,
            a.in_price=b.in_price,a.sales_price=b.sales_price,a.member_price=b.member_price,
            a.product_state=b.product_state,a.updated_at=now();
       commit;

       insert into dim_product(product_code,barcode,product_name,categoryI_code,categoryI_name,categoryII_code,categoryII_name,categoryIII_code,
               categoryIII_name,brand_code,brand_name,in_price,sales_price,member_price,product_state,pos_created_at)
       select a.product_code,replace(a.barcode,'\n',''),replace(a.product_name,'\n',''),a.categoryI_code,a.categoryI_name,a.categoryII_code,a.categoryII_name,a.categoryIII_code,
               a.categoryIII_name,a.brand_code,a.brand_name,a.in_price,a.sales_price,a.member_price,a.product_state,a.pos_created_at
       from tmp_product as a
       left join dim_product  as b on a.product_code = b.product_code 
       where b.product_code is null;
       commit; 

       insert into dim_product_brand(brand_code,brand_name)
       select a.* from 
         (select brand_code, brand_name from tmp_product group by brand_code, brand_name) a
         left join dim_product_brand b on a.brand_code=b.brand_code 
         where b.brand_code is null;

       insert into dim_product_category(category_code, category_name,category_type)
       select a.*, 1 as category_type from 
         (select  categoryI_code, categoryI_name from tmp_product group by categoryI_code, categoryI_name) a
         left join (select category_code from dim_product_category where category_type=1) b on a.categoryI_code=b.category_code 
         where  b.category_code is null;

       insert into dim_product_category( category_code, category_name,category_type)
       select a.*, 2 as category_type from 
         (select categoryII_code, categoryII_name from tmp_product group by categoryII_code, categoryII_name) a
         left join (select category_code from dim_product_category where category_type=2) b on a.categoryII_code=b.category_code 
         where  b.category_code is null;

       insert into dim_product_category(category_code, category_name,category_type)
       select a.*, 3 as category_type from 
         (select categoryIII_code, categoryIII_name from tmp_product group by categoryIII_code, categoryIII_name) a
         left join (select category_code from dim_product_category where category_type=3) b on a.categoryIII_code=b.category_code 
         where  b.category_code is null;

      update dim_product_category set std_category_code='05' where category_code like '01%' and category_type>1;
      update dim_product_category set std_category_code='06' where category_code like '02%' and category_type>1;
      update dim_product_category set std_category_code='09' where category_code like '03%' and category_type>1;
      update dim_product_category set std_category_code='08' where category_code like '04%' and category_type>1;
      update dim_product_category set std_category_code='08' where category_code like '05%' and category_type>1;
      update dim_product_category set std_category_code='07' where category_code like '06%' and category_type>1;
      update dim_product_category set std_category_code='06' where category_code like '07%' and category_type>1;
      update dim_product_category set std_category_code='18' where category_code like '08%' and category_type>1;
      update dim_product_category set std_category_code='90' where category_code like '09%' and category_type>1;
      update dim_product_category set std_category_code='09' where category_code like '10%' and category_type>1;
      update dim_product_category set std_category_code='10' where category_code like '11%' and category_type>1;
      update dim_product_category set std_category_code='01' where category_code like '12%' and category_type>1;
      update dim_product_category set std_category_code='02' where category_code like '13%' and category_type>1;
      update dim_product_category set std_category_code='02' where category_code like '14%' and category_type>1;
      update dim_product_category set std_category_code='03' where category_code like '15%' and category_type>1;
      update dim_product_category set std_category_code='18' where category_code like '18%' and category_type>1;
      update dim_product_category set std_category_code='04' where category_code like '20%' and category_type>1;
      update dim_product_category set std_category_code='04' where category_code like '21%' and category_type>1;
      update dim_product_category set std_category_code='10' where category_code like '22%' and category_type>1;
      update dim_product_category set std_category_code='04' where category_code like '23%' and category_type>1;
      update dim_product_category set std_category_code='09' where category_code like '24%' and category_type>1;
      update dim_product_category set std_category_code='10' where category_code like '25%' and category_type>1;
      update dim_product_category set std_category_code='10' where category_code like '26%' and category_type>1;
      update dim_product_category set std_category_code='04' where category_code like '27%' and category_type>1;
      update dim_product_category set std_category_code='90' where category_code like '99%' and category_type>1;

      update dim_product_category set std_category_name='奶粉' where std_category_code='01';
      update dim_product_category set std_category_name='营养辅食' where std_category_code='02';
      update dim_product_category set std_category_name='尿布尿裤' where std_category_code='03';
      update dim_product_category set std_category_name='童装童鞋' where std_category_code='04';
      update dim_product_category set std_category_name='喂养用品' where std_category_code='05';
      update dim_product_category set std_category_name='洗护用品' where std_category_code='06';
      update dim_product_category set std_category_name='车床出行' where std_category_code='07';
      update dim_product_category set std_category_name='玩具文娱' where std_category_code='08';
      update dim_product_category set std_category_name='妈妈用品' where std_category_code='09';
      update dim_product_category set std_category_name='婴儿棉品' where std_category_code='10';
      update dim_product_category set std_category_name='其他' where std_category_code='90';
      update dim_product_category set std_category_name='券' where std_category_code='91';
      update dim_product_category set std_category_name='服务类' where std_category_code='18';

      update dim_product a 
      inner join dim_product_category b on a.categoryii_code=b.category_code and b.category_type=2 
      set a.std_categoryi_code = b.std_category_code, a.std_categoryi_name = b.std_category_name
      where a.std_categoryi_code is null;

      update dim_product a 
      inner join dim_product_brand b on a.brand_name=b.brand_name 
      set a.brand_code = b.brand_code
      where a.brand_code is null;

      update dim_product set std_categoryi_code='90',std_categoryi_name='其他' where product_name like '%赠品%';
      update dim_product set std_categoryi_code='91',std_categoryi_name='券' where product_name like '%券%';
      update dim_product set std_categoryi_code='91',std_categoryi_name='券' where product_name like '%补差价%';
      commit;

      update fct_sales AS a
      INNER JOIN dim_product AS b ON a.dim_product_id = b.dim_product_id 
      set a.std_categoryi_code=b.std_categoryi_code 
      where a.std_categoryi_code is null;
      commit;

       update tmp_member set create_date='1990-01-01' where create_date like '0000%';
       update tmp_member set baby_birthday='1990-01-01' where baby_birthday like '0000%';
       update tmp_member set member_code=trim(member_code),card_no=trim(card_no),mobile=trim(mobile);
       update tmp_member set mobile=null where LENGTH(trim(mobile))!=11 and substr(mobile,1,1)!='1';
       commit;

       update tmp_member set member_no=card_no;
       update dim_member a 
       inner join tmp_member  b on a.member_code=b.member_code
       set a.member_no = b.member_no, a.card_no=b.card_no,
           a.member_name=b.member_name,a.baby_birthday=ifnull(b.baby_birthday,'1990-01-01'),
           a.member_state=b.member_state,a.mobile=b.mobile,
           a.member_type=b.member_type,a.member_type_name=b.member_type_name,
           a.create_date=b.create_date,a.reg_shop_code=b.reg_shop_code,
           a.updated_at=now();
       commit;

       insert into dim_member(member_code,member_no,card_no,member_name,baby_birthday,member_type,member_type_name,mobile,
            reg_shop_code,create_date,member_state,app_flag,app_reg_date)
       select a.member_code,a.member_no,a.card_no,a.member_name,a.baby_birthday,a.member_type,a.member_type_name,a.mobile,
            a.reg_shop_code,a.create_date,a.member_state,a.app_flag,a.app_reg_date
        from tmp_member as a
        left join dim_member  as b on a.member_code = b.member_code 
       where b.member_code is null;
       commit;

       update dim_member set normal_member=0 where member_state<>'1' ;
       commit;

       update tmp_guider set guider_code=trim(guider_code);
       update dim_guider a 
       inner join tmp_guider  b on a.guider_code=b.guider_code
       set a.guider_name = b.guider_name, a.guider_mobile=b.guider_mobile,
           a.shop_codes=b.shop_codes,a.status=b.status,
           a.memo=b.memo,a.updated_at=now();
       commit;

       insert into dim_guider(guider_code,guider_name,guider_mobile,shop_codes,status,memo)
       select a.guider_code,a.guider_name,a.guider_mobile,a.shop_codes,a.status,a.memo
        from tmp_guider as a
        left join dim_guider  as b on a.guider_code=b.guider_code
       where b.guider_code is null;
       commit;

       update tmp_payment set payment_way=trim(payment_way);
       update dim_payment a 
       inner join tmp_payment  b on a.payment_way=b.payment_way and a.payment_flag=b.payment_flag
       set a.payment_name = b.payment_name, a.jf_flag=b.jf_flag,
           a.memo=b.memo,a.status=b.status,
           a.updated_at=now();
       commit;

       insert into dim_payment(payment_way,payment_name,payment_flag,jf_flag,status,memo)
       select a.payment_way,a.payment_name,a.payment_flag,a.jf_flag,a.status,a.memo
        from tmp_payment as a
        left join dim_payment  as b on a.payment_way=b.payment_way and a.payment_flag=b.payment_flag
       where b.payment_way is null;
       commit;

       insert into event_log(event_name,event_type,event_date) values('deal_dim','end',now());
       update event_log set event_type='2' where event_name='importdata' and date_format(event_date,'%Y%m%d')=date_format(curdate(),'%Y%m%d');
  end if;
end