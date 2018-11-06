DROP PROCEDURE IF EXISTS `DM_Member_PG`;
CREATE PROCEDURE `DM_Member_PG`(in `interval_m`  int)
BEGIN

	SET @edate_id=DATE_FORMAT(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY),'%Y%m%d'), @sdate_id=DATE_FORMAT(DATE_SUB(CURRENT_DATE,INTERVAL interval_m MONTH),'%Y%m%d');  

	TRUNCATE TABLE dm_member_mba_g;
	
	INSERT INTO dm_member_mba_g(member_no, G01, G02, G03, G04, G05, G06, G07, G08, G09, G10)
	SELECT member_no, SUM(G01), SUM(G02), SUM(G03), SUM(G04), SUM(G05), SUM(G06), SUM(G07), SUM(G08), SUM(G09), SUM(G10) FROM
	(
		SELECT member_no, 
			CASE group_code WHEN 'G01' THEN 1 ELSE 0 END AS G01,
			CASE group_code WHEN 'G02' THEN 1 ELSE 0 END AS G02,
			CASE group_code WHEN 'G03' THEN 1 ELSE 0 END AS G03,
			CASE group_code WHEN 'G04' THEN 1 ELSE 0 END AS G04,
			CASE group_code WHEN 'G05' THEN 1 ELSE 0 END AS G05,
			CASE group_code WHEN 'G06' THEN 1 ELSE 0 END AS G06,
			CASE group_code WHEN 'G07' THEN 1 ELSE 0 END AS G07,
			CASE group_code WHEN 'G08' THEN 1 ELSE 0 END AS G08,
			CASE group_code WHEN 'G09' THEN 1 ELSE 0 END AS G09,
			CASE group_code WHEN 'G10' THEN 1 ELSE 0 END AS G10
		FROM 
		(
					SELECT si.member_no, pg.group_code FROM his_sales_item si, lkp_product_group pg
					WHERE 
						dim_date_id BETWEEN @sdate_id AND @edate_id
						AND si.product_code=pg.product_code
					GROUP BY si.member_no, pg.group_code
		) dx
	) dy
	GROUP BY member_no;

END