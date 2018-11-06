DROP PROCEDURE IF EXISTS `DM`;
CREATE PROCEDURE `DM`()
BEGIN
	
	call DM_Member();
	call DM_Member_PG(12);
	call DM_Member_NFPG(6);
END