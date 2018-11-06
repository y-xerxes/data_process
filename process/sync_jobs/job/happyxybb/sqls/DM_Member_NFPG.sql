DROP PROCEDURE IF EXISTS `DM_Member_NFPG`;
CREATE PROCEDURE `DM_Member_NFPG`(in `interval_m`  int)
BEGIN

	SET @edate_id=DATE_FORMAT(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY),'%Y%m%d'), @sdate_id=DATE_FORMAT(DATE_SUB(CURRENT_DATE,INTERVAL interval_m MONTH),'%Y%m%d');  

	TRUNCATE TABLE stg_member_nfls;
	
	INSERT INTO stg_member_nfls(dim_member_id,member_no,group_code,last_shopping,last_unit_quantity,spent_day,expired_date )
	SELECT dim_member_id,member_no,group_code,last_shopping,last_unit_quantity,spent_day,expired_date FROM
	(
	SELECT fs.dim_member_id, dm.member_no, sg.group_code, fs.dim_date_id AS last_shopping, SUM(fs.quantity*sg.unit) AS last_unit_quantity, SUM(fs.quantity*sg.unit_day) AS spent_day, DATE_ADD(STR_TO_DATE(dim_date_id,'%Y%m%d'), INTERVAL SUM(fs.quantity*sg.unit_day) DAY) AS expired_date
	FROM fct_sales fs 
	JOIN lkp_product_nf_spec_group sg ON  fs.dim_product_id=sg.dim_product_id
	JOIN dim_member dm ON fs.dim_member_id=dm.dim_member_id AND dm.abnormal=0
	WHERE	
		fs.dim_date_id BETWEEN @sdate_id AND @edate_id
	GROUP BY
		fs.dim_member_id, fs.dim_date_id, sg.group_code
	ORDER BY last_shoping DESC
	) dx
	GROUP BY
		dx.dim_member_id, dx.group_code;

	UPDATE stg_member_nfls mr, lkp_enum_scope s
	SET mr.NFLS=s.enum_value
	WHERE
		DATEDIFF(expired_date, CURDATE()) BETWEEN s.min_value AND s.max_value
	AND s.type='NFLS';

END