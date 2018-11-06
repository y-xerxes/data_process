DROP PROCEDURE IF EXISTS `Get_US_RFM_Members`;
CREATE PROCEDURE `Get_US_RFM_Members`(in `scenario`  text, in `activity` text, in `exclusive` text, in `p`  text, in `op` text, in `orderby` int, in `offset` int ,in `limit` int, in `org_code` varchar(50))
BEGIN
	
 
 
 DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET @error=1;
 SET @scenario_code = scenario, @activity_code = activity, @exclusive_id = `exclusive`, @org_code=`org_code`;
 SET @required = IF(RIGHT(`p`,1)=';',`p`,CONCAT(`p`,';')), @optionals = IF(RIGHT(`op`,1)=';',`op`,CONCAT(`op`,';'));

 CREATE TEMPORARY TABLE map(k varchar(16), v VARCHAR(32), seq int(11) AUTO_INCREMENT,PRIMARY KEY (seq));
 IF (@error=1) THEN
	DROP table map;
	CREATE TEMPORARY TABLE map(k varchar(16), v VARCHAR(32), seq int(11) AUTO_INCREMENT,PRIMARY KEY (seq));
 END IF;

 SET @split=';', @c=@required; 
 WHILE (LOCATE(@split,@c)<>0) DO
	INSERT INTO map(k) VALUES (LEFT(@c,LOCATE(@split,@c)-1));
	SET @c = SUBSTR(@c,LOCATE(@split,@c)+1);
 END WHILE;
 
 SET @c=@optionals;
 WHILE (LOCATE(@split,@c)<>0) DO
	INSERT INTO map(k) VALUES (LEFT(@c,LOCATE(@split,@c)-1));
	SET @c = SUBSTR(@c,LOCATE(@split,@c)+1);
 END WHILE;

 
 
 SET @s_date_index  = LOCATE("s=",@required,1),  @e_date_index  = LOCATE("e=",@required) ;
 SET @s_date_value  = SUBSTRING(SUBSTRING(@required FROM @s_date_index),LENGTH("s=")+1,LOCATE(";",SUBSTRING(@required FROM @s_date_index),1)-LENGTH("s=")-1);
 SET @e_date_value  = SUBSTRING(SUBSTRING(@required FROM @e_date_index),LENGTH("e=")+1,LOCATE(";",SUBSTRING(@required FROM @e_date_index),1)-LENGTH("e=")-1);

 
 SET @es_date_index = LOCATE("e_s=",@required),  @ee_date_index = LOCATE("e_e=",@required) ;
 SET @es_date_value  = SUBSTRING(SUBSTRING(@required FROM @es_date_index),LENGTH("e_s=")+1,LOCATE(";",SUBSTRING(@required FROM @es_date_index),1)-LENGTH("e_s=")-1);
 SET @ee_date_value  = SUBSTRING(SUBSTRING(@required FROM @ee_date_index),LENGTH("e_e=")+1,LOCATE(";",SUBSTRING(@required FROM @ee_date_index),1)-LENGTH("e_e=")-1);

 
 SET @pt_index = LOCATE("pt=",@required), @pv_index = LOCATE("pv=",@required);
 SET @pt_index_value  = SUBSTRING(SUBSTRING(@required FROM @pt_index),LENGTH("pt=")+1,LOCATE(";",SUBSTRING(@required FROM @pt_index),1)-LENGTH("pt=")-1);
 SET @pv_index_value  = SUBSTRING(SUBSTRING(@required FROM @pv_index),LENGTH("pv=")+1,LOCATE(";",SUBSTRING(@required FROM @pv_index),1)-LENGTH("pv=")-1);

 
 SET @r_index = LOCATE("R=",@optionals), @f_index = LOCATE("F=",@optionals), @m_index = LOCATE("M=",@optionals), 
		 @ac_index = LOCATE("AC=",@optionals), @nc_index = LOCATE("NC=",@optionals), @nbc_index = LOCATE("NBC=",@optionals), 
		 @fmcg_index = LOCATE("FMCG=",@optionals);
 SET @r_index_value  = SUBSTRING(SUBSTRING(@optionals FROM @r_index),LENGTH("R=")+1,LOCATE(";",SUBSTRING(@optionals FROM @r_index),1)-LENGTH("R=")-1);
 SET @f_index_value  = SUBSTRING(SUBSTRING(@optionals FROM @f_index),LENGTH("F=")+1,LOCATE(";",SUBSTRING(@optionals FROM @f_index),1)-LENGTH("F=")-1);
 SET @m_index_value  = SUBSTRING(SUBSTRING(@optionals FROM @m_index),LENGTH("M=")+1,LOCATE(";",SUBSTRING(@optionals FROM @m_index),1)-LENGTH("M=")-1);
 SET @ac_index_value  = SUBSTRING(SUBSTRING(@optionals FROM @ac_index),LENGTH("AC=")+1,LOCATE(";",SUBSTRING(@optionals FROM @ac_index),1)-LENGTH("AC=")-1);
 SET @nc_index_value  = SUBSTRING(SUBSTRING(@optionals FROM @nc_index),LENGTH("NC=")+1,LOCATE(";",SUBSTRING(@optionals FROM @nc_index),1)-LENGTH("NC=")-1);
 SET @nbc_index_value  = SUBSTRING(SUBSTRING(@optionals FROM @nbc_index),LENGTH("NBC=")+1,LOCATE(";",SUBSTRING(@optionals FROM @nbc_index),1)-LENGTH("NBC=")-1);
 SET @fmcg_index_value  = SUBSTRING(SUBSTRING(@optionals FROM @fmcg_index),LENGTH("FMCG=")+1,LOCATE(";",SUBSTRING(@optionals FROM @fmcg_index),1)-LENGTH("FMCG=")-1);
 

 SET @province_index  = LOCATE("province=",@optionals);
 SET @province_value  = SUBSTRING(SUBSTRING(@optionals FROM @province_index),LENGTH("province=")+1,LOCATE(";",SUBSTRING(@optionals FROM @province_index),1)-LENGTH("province=")-1);

 
 SET @age_index  = LOCATE("week_age=",@optionals);
 SET @age_value  = SUBSTRING(SUBSTRING(@optionals FROM @age_index),LENGTH("week_age=")+1,LOCATE(";",SUBSTRING(@optionals FROM @age_index),1)-LENGTH("week_age=")-1);

 
 SET @s1_date_id = DATE_FORMAT(@s_date_value,"%Y%m%d"), @e1_date_id = DATE_FORMAT(@e_date_value,"%Y%m%d"), @s2_date_id = DATE_FORMAT(@es_date_value,"%Y%m%d"), @e2_date_id = DATE_FORMAT(@ee_date_value,"%Y%m%d");

 

 
 
 
 
 

 
 IF (@pt_index_value=0) THEN SET @Goods_Type_ID= 'si.product_code';
   SET @sql=CONCAT('SELECT IFNULL(GROUP_CONCAT(dg.product_code),0) INTO @goods FROM dim_product dg WHERE dg.barcode in (',@pv_index_value,');');
 
 ELSEIF(@pt_index_value=1) THEN  SET @Goods_Type_ID= 'si.product_code';
   SET @sql=CONCAT('SELECT IFNULL(GROUP_CONCAT(dgt.product_code),0) INTO @goods FROM dim_product dgt WHERE dgt.categoryi_code in (',@pv_index_value,')');
 
 ELSEIF(@pt_index_value=2) THEN  SET @Goods_Type_ID= 'si.product_code';
   SET @sql=CONCAT('SELECT IFNULL(GROUP_CONCAT(dgt.product_code),0) INTO @goods FROM dim_product dgt WHERE dgt.categoryii_code in (',@pv_index_value,')');
 
 ELSEIF(@pt_index_value=3) THEN  SET @Goods_Type_ID= 'si.product_code';
   SET @sql=CONCAT('SELECT IFNULL(GROUP_CONCAT(dgt.product_code),0) INTO @goods FROM dim_product dgt WHERE dgt.categoryiii_code in (',@pv_index_value,')');
 END IF;

 
 
 IF(@pt_index_value<>5) THEN
	
  PREPARE stmt FROM @sql;EXECUTE stmt;DEALLOCATE PREPARE stmt;
 END IF;


 
 
 SET @seq:=0, @c_time:=CURRENT_TIMESTAMP, @date_id:=DATE_FORMAT(CURRENT_DATE,'%Y%m%d');
 SET @url = CONCAT("URL=scenario=",@scenario_code,"&activity_code=",@activity_code,"&exclusive=",@exclusive_id,"&p=",p,"&op=",op,"&order_by=",orderby,"&offset=",`offset`,"&limit=",`limit`, "&org_code=",@org_code);
 INSERT INTO ins_a01_rfm_activity (scenario, date_id, activity_code, conditions, mark_time) VALUES (@scenario_code, @date_id, @activity_code,@url,@c_time);

 
 
 
 
 SELECT LAST_INSERT_ID() INTO @seq;
 SET @tflag:=0; 

 SET @cflag=0;
 
 
 
 IF(@pt_index>0) THEN
    IF(@pt_index_value=5) THEN

		SELECT COUNT(1) INTO @cflag FROM map WHERE k like 'NFLS_%';
			IF(@cflag>0) THEN 
				SELECT SUBSTR(k, LENGTH('NFLS_SGXX=')+1) INTO @p_value FROM map WHERE k like 'NFLS_%';
				SET @sql0=CONCAT('
					 INSERT INTO ins_a01_rfm_member_options (seq,activity_code, member_no,flag)
					 SELECT ',@seq,' as seq,"',@activity_code,'" AS activity_code,si.member_no,0 as flag
					 FROM stg_member_nfls si
					 WHERE
							si.NFLS IN (',@p_value,') 
					 GROUP BY si.member_no;');
				
			ELSE 
				SET @sql0=CONCAT('
					 INSERT INTO ins_a01_rfm_member_options (seq,activity_code, member_no,flag)
					 SELECT ',@seq,' as seq,"',@activity_code,'" AS activity_code,si.member_no,0 as flag
					 FROM his_sales_item si
					 WHERE
							si.dim_date_id BETWEEN ',@s1_date_id,' AND ',@e1_date_id,' 
					 GROUP BY si.member_no;');
			END IF;
    ELSEIF(@s_date_index >0 && @e_date_index >0 && @pv_index >0) THEN
    SET @sql0=CONCAT('
       INSERT INTO ins_a01_rfm_member_options (seq,activity_code, member_no,flag)
       SELECT ',@seq,' as seq,"',@activity_code,'" AS activity_code,si.member_no,0 as flag
       FROM his_sales_item si
       WHERE
         si.dim_date_id BETWEEN ',@s1_date_id,' AND ',@e1_date_id,' 
         AND ',@Goods_Type_ID,' IN (',@goods,')
       GROUP BY si.member_no;');
    END IF;

    
    PREPARE stmt FROM @sql0;EXECUTE stmt;DEALLOCATE PREPARE stmt;
 END IF;


 
 
 
 

 IF(@es_date_index >0 && @ee_date_index >0 && @pt_index>0) THEN
    IF(@pt_index_value=5) THEN
			SET @sql1=CONCAT('
				 DELETE a.* FROM ins_a01_rfm_member_options as a,
				 (
						SELECT  member_no
						FROM his_sales_item
						WHERE  dim_date_id BETWEEN ',@s2_date_id,' AND ',@e2_date_id,' 
						GROUP BY member_no
				 ) as b
				 WHERE a.member_no=b.member_no and a.seq=',@seq,';');
    ELSEIF(@pv_index >0) THEN
				SET @sql1=CONCAT('
				DELETE a.* FROM ins_a01_rfm_member_options as a,
				 (
						SELECT  member_no
						FROM his_sales_item
						WHERE  dim_date_id BETWEEN ',@s2_date_id,' AND ',@e2_date_id,' 
            AND ',@Goods_Type_ID,' IN (',@goods,')
						GROUP BY member_no
				 ) as b
				 WHERE a.member_no=b.member_no and a.seq=',@seq,';');
    END IF;
    
    PREPARE stmt FROM @sql1;EXECUTE stmt;DEALLOCATE PREPARE stmt;
 END IF;

 
 
 





 
 
 

SET @condition = CONCAT(' mo.member_no = mr.member_no AND mr.abnormal=0 AND mo.seq = ',@seq); 
SET @flag = 0;
 
 
 IF(@age_index >0) THEN
    SET @condition = CONCAT(@condition, ' AND mr.week_age_value IN (',@age_value,')'); 
		SET @flag = 1;
 END IF;
 
 
 IF(@r_index >0) THEN  
    SET @condition = CONCAT(@condition,' AND mr.R=',@r_index_value);
		SET @flag = 1;
 END IF;
 
 
 IF(@f_index >0) THEN
		SET @condition = CONCAT(@condition,' AND mr.F=',@f_index_value); 
		SET @flag = 1;
 END IF;
 
 
 IF(@m_index >0) THEN
    SET @condition = CONCAT(@condition,' AND mr.M=',@m_index_value); 
		SET @flag = 1;
 END IF;

 
 IF(@ac_index >0) THEN
    SET @condition = CONCAT(@condition,' AND mr.AC IN (',@ac_index_value,')'); 
		SET @flag = 1;
 END IF;

 
 IF(@nc_index >0) THEN
    SET @condition = CONCAT(@condition,' AND mr.NC IN (',@nc_index_value,')'); 
		SET @flag = 1;
 END IF;


IF(@flag=1) THEN
	 SET @tflag:=@tflag+1;
	 SET @sql = CONCAT('
				 UPDATE ins_a01_rfm_member_options AS mo, dm_member_rfm AS mr
				 SET mo.flag = mo.flag + 1
				 WHERE ', @condition);
	 
	 PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
END IF;

 
 
 
 
 SET @condition = CONCAT(' mo.member_no = mba.member_no AND mo.seq = ',@seq); 
 SET @sub_condition='';
 SET @flag=0;

 
 IF(@nbc_index >0) THEN
    SET @tmp_sql = CONCAT('SELECT CONCAT(" AND (",GROUP_CONCAT(query_criteria SEPARATOR " OR "),")") INTO @sub_condition FROM lkp_enum_scope WHERE type="NBC" AND code IN (',@nbc_index_value,')'); 
		PREPARE stmt FROM @tmp_sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
		
		SET @condition = CONCAT(@condition, @sub_condition);
		SET @flag=1;
 END IF;

 IF(@fmcg_index >0) THEN
    SELECT CONCAT(" AND (",query_criteria,")") INTO @sub_condition FROM lkp_enum_scope WHERE type="FMCG" AND code = @fmcg_index_value; 
		
		SET @condition = CONCAT(@condition, @sub_condition);
		SET @flag=1;
 END IF;


IF(@flag=1) THEN
	 SET @tflag:=@tflag+1;
	 SET @sql = CONCAT('
				 UPDATE ins_a01_rfm_member_options AS mo, dm_member_mba AS mba
				 SET mo.flag = mo.flag + 1
				 WHERE ', @condition);
	 
	 PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
 END IF;

 
 
 
 SET @condition = CONCAT(' mo.member_no = mba.member_no AND mo.seq = ',@seq,' '); 
 SET @sub_condition='';
 SET @flag=0;


SELECT GROUP_CONCAT(SUBSTR(k,LENGTH('NBG_')+1) SEPARATOR ' AND ') INTO @sub_condition FROM map WHERE k like 'NBG_%';
	IF(LENGTH(@sub_condition)>0) THEN
		SET @condition = CONCAT(@condition,' AND ', @sub_condition);
	 SET @tflag:=@tflag+1;
	 SET @sql = CONCAT('
				 UPDATE ins_a01_rfm_member_options AS mo, dm_member_mba_g AS mba
				 SET mo.flag = mo.flag + 1
				 WHERE ', @condition);
	 PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
 END IF;


 
 
 
 
 
 
 
 IF(`exclusive` <> 0) THEN
		DELETE a.* FROM ins_a01_rfm_member_options a, ins_a01_rfm_member b
		WHERE 
			a.seq=@seq AND a.member_no=b.member_no AND b.activity_code=@activity_code;
 END IF;


 SET @orderby_column = CASE `orderby`
   WHEN 1 THEN 'mr.total_cost'
   WHEN 2 THEN 'mr.order_date_num'
   WHEN 3 THEN 'mr.last_shopping_date'
   ELSE 'mr.total_cost'
 END;

    SET @backup_sql = CONCAT('
       INSERT INTO ins_a01_rfm_member (seq, date_id, activity_code, member_id, member_no)
       SELECT mo.seq,',@date_id,' AS date_id,mo.activity_code,mo.member_id,mo.member_no
       FROM ins_a01_rfm_member_options AS mo
       INNER JOIN dm_member_rfm AS mr ON mo.member_no = mr.member_no
       WHERE mo.seq=',@seq,' AND mo.flag = ',@tflag,' 
       ORDER BY ',@orderby_column,' DESC
       LIMIT ',`offset`,' , ',`limit`,';
    ');
 
 PREPARE stmt FROM @backup_sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

 DELETE mo.* FROM ins_a01_rfm_member_options AS mo WHERE mo.seq = @seq;

 SET @sub_condition='';
 SELECT k INTO @sub_condition FROM map WHERE k = 'EX=1';
	IF(LENGTH(@sub_condition)>0) THEN
		 SELECT mm.member_no, rfm.mobile, rfm.total_cost, rfm.order_date_num, rfm.last_shopping_date 
		 FROM ins_a01_rfm_member mm, dm_member_rfm rfm
		 WHERE mm.activity_code=@activity_code AND mm.seq = @seq AND mm.member_no=rfm.member_no AND rfm.mobile is not null;
  ELSE
		 SELECT mm.member_no
		 FROM ins_a01_rfm_member mm, dm_member_rfm rfm
		 WHERE mm.activity_code=@activity_code AND mm.seq = @seq AND mm.member_no=rfm.member_no AND rfm.mobile is not null;
  END IF;


DROP TABLE map;

 
 END