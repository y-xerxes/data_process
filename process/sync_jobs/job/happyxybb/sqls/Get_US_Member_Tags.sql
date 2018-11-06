DROP PROCEDURE IF EXISTS `Get_US_Member_Tags`;
CREATE PROCEDURE `Get_US_Member_Tags`(IN `member_no` varchar(4096), IN org_code varchar(32))
BEGIN
	SET @sql=CONCAT('
	SELECT rfm.member_no, rfm.week_age_value AS WA, rfm.R, rfm.F, rfm.M, rfm.AC, rfm.NC,
		mbac.C01, mbac.C02, mbac.C03, mbac.C04, mbac.C05, mbac.C06, mbac.C07, mbac.C08, mbac.C09, mbac.C10, 
		mbag.G01, mbag.G02, mbag.G03, mbag.G04, mbag.G05, mbag.G06, mbag.G07, mbag.G08, mbag.G09, mbag.G10 
	FROM dm_member_rfm rfm
	LEFT JOIN dm_member_mba mbac ON rfm.member_no=mbac.member_no
	LEFT JOIN dm_member_mba_g mbag ON rfm.member_no=mbag.member_no
	WHERE 
		rfm.abnormal=0 AND rfm.member_no IN (',`member_no`,')');  
	PREPARE stmt FROM @sql;EXECUTE stmt;DEALLOCATE PREPARE stmt;
END