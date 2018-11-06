DROP PROCEDURE IF EXISTS `tmp_sp_month_data`;
CREATE PROCEDURE `tmp_sp_month_data`()
BEGIN


SET @date:='2015-01-01';
WHILE @date<='2017-04-01' DO
SET @date:=DATE_ADD(@date,INTERVAL 1 MONTH);
#SET @date:='2016-08-01';
SET @sdate:=(SELECT min(dim_date_id) FROM dim_date_day 
     where substr(dim_date_id,1,6)= DATE_FORMAT(date_sub(@date,interval 1 month),'%Y%m'));
SET @edate:=(SELECT max(dim_date_id) FROM dim_date_day 
     where substr(dim_date_id,1,6)= DATE_FORMAT(date_sub(@date,interval 1 month),'%Y%m'));
SET @sdate_i:=DATE_FORMAT(DATE_SUB(@sdate,INTERVAL 1 YEAR),'%Y%m%d');
SET @edate_i:=DATE_FORMAT(@edate,'%Y%m%d');
SET @edate_ii:=DATE_FORMAT(DATE_SUB(@edate,INTERVAL 3 MONTH),'%Y%m%d');
SET @sdate_ii:=DATE_FORMAT(DATE_SUB(@sdate,INTERVAL 3 MONTH),'%Y%m%d');

DROP  TABLE IF EXISTS t_member;
CREATE  TABLE IF NOT EXISTS t_member(  `dim_member_id` VARCHAR(32) DEFAULT '0',KEY `IDX_member` (`dim_member_id`) USING BTREE);

INSERT INTO t_member
SELECT dim_member_id
  FROM dim_member a
  JOIN dim_shop b ON a.reg_shop_code=b.shop_code AND b.`online`=1
 WHERE first_shoping <DATE_SUB(@sdate,INTERVAL 2 MONTH)
   AND dim_member_id<>0
;
delete from sum_month_old_customer where yearmonth=DATE_FORMAT(@sdate,'%Y%m');
insert into sum_month_old_customer
SELECT DATE_FORMAT(@sdate,'%Y%m'),COUNT(DISTINCT dim_member_id) AS total_old_member,COUNT(DISTINCT CASE WHEN max_date BETWEEN @sdate_ii AND @edate_ii THEN dim_member_id ELSE NULL END) AS no_active_old_member_num
  FROM
(
SELECT a.dim_member_id,MAX(dim_date_id) AS max_date
  FROM fct_sales AS a
  JOIN t_member AS b ON a.dim_member_id=b.dim_member_id
 WHERE dim_date_id BETWEEN @sdate_i AND @edate_i
 GROUP BY a.dim_member_id
) a;

END WHILE;
end