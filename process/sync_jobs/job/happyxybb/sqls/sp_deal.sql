DROP PROCEDURE IF EXISTS `sp_deal`;
CREATE PROCEDURE `sp_deal`()
BEGIN
	call sp_deal_dim;
  call sp_deal_fct;
  call sp_submit_fct;

END