DROP PROCEDURE IF EXISTS `DM_Member`;
CREATE PROCEDURE `DM_Member`()
BEGIN

	SET @edate_id=DATE_FORMAT(DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY),'%Y%m%d'), @sdate_id=DATE_FORMAT(DATE_SUB(CURRENT_DATE,INTERVAL 1 YEAR),'%Y%m%d');
  
	TRUNCATE TABLE dm_member_mba;
	TRUNCATE TABLE dm_member_rfm;

	INSERT INTO dm_member_rfm (member_no, mobile, birthday, week_age_value, first_shopping_date,last_shopping_date, order_date_num, order_num, total_cost, abnormal)
	SELECT m.member_no, m.mobile, m.baby_birthday, FLOOR(DATEDIFF(CURRENT_DATE,m.baby_birthday)/7) AS week_age_value, 
		STR_TO_DATE(MIN(dim_date_id),'%Y%m%d') AS first_shopping_date, STR_TO_DATE(MAX(dim_date_id),'%Y%m%d') AS last_shopping_date, COUNT(DISTINCT dim_date_id) AS order_date_num, 
		COUNT(DISTINCT order_no) AS order_num, SUM(amount) AS total_cost, m.abnormal
	FROM fct_sales fs, dim_member m
	WHERE
			 fs.dim_date_id BETWEEN @sdate_id AND @edate_id
			 AND fs.dim_member_id=m.dim_member_id
	GROUP BY fs.dim_member_id;

	INSERT INTO dm_member_rfm (member_no, mobile, birthday, week_age_value)
	SELECT member_no, mobile, baby_birthday, FLOOR(DATEDIFF(CURRENT_DATE,baby_birthday)/7) AS week_age_value FROM `dim_member` dm
	where create_date>DATE_SUB(CURRENT_DATE,INTERVAL 1 YEAR) AND dm.member_no NOT IN (SELECT member_no FROM dm_member_rfm);

	UPDATE dm_member_rfm mr, lkp_enum_scope s
	SET mr.NC=s.enum_value
	WHERE
		DATEDIFF(CURDATE(),first_shopping_date) BETWEEN s.min_value AND s.max_value
	AND s.type='NC';

	UPDATE dm_member_rfm mr, lkp_enum_scope s
	SET mr.AC=s.enum_value
	WHERE
		DATEDIFF(CURDATE(),last_shopping_date) BETWEEN s.min_value AND s.max_value
	AND s.type='AC';

	UPDATE dm_member_rfm rfm, lkp_enum_scope s
	SET rfm.R=s.enum_value
	WHERE
		DATEDIFF(CURDATE(),last_shopping_date) BETWEEN s.min_value AND s.max_value
	AND s.type='R';

	UPDATE dm_member_rfm rfm, lkp_enum_scope s
	SET rfm.F=s.enum_value
	WHERE
		order_date_num BETWEEN s.min_value AND s.max_value
	AND s.type='F';

	UPDATE dm_member_rfm rfm, lkp_enum_scope s
	SET rfm.M=s.enum_value
	WHERE
		total_cost BETWEEN s.min_value AND s.max_value
	AND s.type='M';

	INSERT INTO dm_member_mba(member_no, C01, C02, C03, C04, C05, C06, C07, C08, C09, C10)
	SELECT member_no, SUM(C01) AS C01, SUM(C02) AS C02, SUM(C03) AS C03, SUM(C04) AS C04, SUM(C05) AS C05, SUM(C06) AS C06, SUM(C07) AS C07, SUM(C08) AS C08, SUM(C09) AS C09, SUM(C10) AS C10
	FROM
	(
		SELECT member_no,
			CASE categoryI WHEN '01' THEN 1 ELSE 0 END AS C01,
			CASE categoryI WHEN '02' THEN 1 ELSE 0 END AS C02,
			CASE categoryI WHEN '03' THEN 1 ELSE 0 END AS C03,
			CASE categoryI WHEN '04' THEN 1 ELSE 0 END AS C04,
			CASE categoryI WHEN '05' THEN 1 ELSE 0 END AS C05,
			CASE categoryI WHEN '06' THEN 1 ELSE 0 END AS C06,
			CASE categoryI WHEN '07' THEN 1 ELSE 0 END AS C07,
			CASE categoryI WHEN '08' THEN 1 ELSE 0 END AS C08,
			CASE categoryI WHEN '09' THEN 1 ELSE 0 END AS C09,
			CASE categoryI WHEN '10' THEN 1 ELSE 0 END AS C10
		FROM
		(
			SELECT fs.dim_member_id, m.member_no, std_categoryi_code AS categoryI 
			FROM fct_sales fs, dim_member m
			WHERE 
				dim_date_id BETWEEN @sdate_id AND @edate_id
				AND fs.dim_member_id=m.dim_member_id
			GROUP BY dim_member_id, std_categoryi_code
		) sx
	) sy
	GROUP BY
			 member_no;
END