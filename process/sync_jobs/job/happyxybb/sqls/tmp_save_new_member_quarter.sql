DROP PROCEDURE IF EXISTS `tmp_save_new_member_quarter`;
CREATE PROCEDURE `tmp_save_new_member_quarter`()
BEGIN
SET @date:='2015-03-01';
DROP  TABLE IF EXISTS sum_month_save_new_customer_quarter;
CREATE TABLE IF NOT EXISTS `sum_month_save_new_customer_quarter` (
  `yearmonth` int(6) DEFAULT NULL,
  `quarter_type` int(1) DEFAULT NULL,
  `save_num` int(11) DEFAULT NULL,
  `amount` float(14,2) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;


WHILE @date<='2017-04-01' DO
SET @date:=DATE_ADD(@date,INTERVAL 1 MONTH);
#SET @date:='2016-08-01';

SET @sdate:=(SELECT min(dim_date_id) FROM dim_date_day 
     where substr(dim_date_id,1,6)= DATE_FORMAT(date_sub(@date,interval 3 month),'%Y%m'));
SET @edate:=(SELECT max(dim_date_id) FROM dim_date_day 
     where substr(dim_date_id,1,6)= DATE_FORMAT(date_sub(@date,interval 3 month),'%Y%m'));
SET @sdate_i=DATE_FORMAT(@sdate,'%Y%m%d');
SET @edate_i=DATE_FORMAT(DATE_SUB(DATE_ADD(@sdate,INTERVAL 3 MONTH),INTERVAL 1 DAY),'%Y%m%d');

DROP  TABLE IF EXISTS t_member;
CREATE  TABLE IF NOT EXISTS t_member(  `dim_member_id` VARCHAR(32) DEFAULT '0',`first_shoping` DATE,KEY `IDX_member` (`dim_member_id`) USING BTREE);

INSERT INTO t_member
SELECT dim_member_id,first_shoping
  FROM dim_member a
  JOIN dim_shop b ON a.reg_shop_code=b.shop_code AND b.`online`=1
 WHERE first_shoping BETWEEN @sdate AND @edate
   AND dim_member_id<>0;

delete from sum_month_save_new_customer_quarter where yearmonth=DATE_FORMAT(@sdate,'%Y%m') AND quarter_type=0;
insert into sum_month_save_new_customer_quarter
SELECT DATE_FORMAT(first_shoping,'%Y%m') AS yearmonth,0 AS quarter_type,COUNT(DISTINCT a.dim_member_id),sum(amount)
  FROM t_member AS b
  JOIN fct_sales AS a ON a.dim_member_id=b.dim_member_id
 WHERE dim_date_id BETWEEN @sdate_i AND @edate_i
 GROUP BY DATE_FORMAT(first_shoping,'%Y%m');

SET @flag:=NULL;
SELECT IFNULL(yearmonth,0) INTO @flag FROM sum_month_save_new_customer_quarter WHERE yearmonth=DATE_FORMAT(@sdate,'%Y%m') AND quarter_type=0;
IF(@flag=0)THEN
	insert into sum_month_save_new_customer_quarter
	  SELECT DATE_FORMAT(@sdate,'%Y%m'),0,IFNULL(@flag,0),IFNULL(@flag,0);
END IF;


SET @sdate:=(SELECT min(dim_date_id) FROM dim_date_day 
     where substr(dim_date_id,1,6)= DATE_FORMAT(date_sub(@date,interval 6 month),'%Y%m'));
SET @edate:=(SELECT max(dim_date_id) FROM dim_date_day 
     where substr(dim_date_id,1,6)= DATE_FORMAT(date_sub(@date,interval 6 month),'%Y%m'));
SET @sdate_i=DATE_FORMAT(DATE_ADD(@sdate,INTERVAL 3 MONTH),'%Y%m%d');
SET @edate_i=DATE_FORMAT(DATE_SUB(DATE_ADD(@sdate,INTERVAL 6 MONTH),INTERVAL 1 DAY),'%Y%m%d');

DROP  TABLE IF EXISTS t_member;
CREATE  TABLE IF NOT EXISTS t_member(  `dim_member_id` VARCHAR(32) DEFAULT '0',`first_shoping` DATE,KEY `IDX_member` (`dim_member_id`) USING BTREE);

INSERT INTO t_member
SELECT dim_member_id,first_shoping
  FROM dim_member a
  JOIN dim_shop b ON a.reg_shop_code=b.shop_code AND b.`online`=1
 WHERE first_shoping BETWEEN @sdate AND @edate
   AND dim_member_id<>0;

delete from sum_month_save_new_customer_quarter where yearmonth=DATE_FORMAT(@sdate,'%Y%m') AND quarter_type=1;
insert into sum_month_save_new_customer_quarter
SELECT DATE_FORMAT(first_shoping,'%Y%m') AS yearmonth,1 AS quarter_type,COUNT(DISTINCT a.dim_member_id),sum(amount)
  FROM t_member AS b
  JOIN fct_sales AS a ON a.dim_member_id=b.dim_member_id
 WHERE dim_date_id BETWEEN @sdate_i AND @edate_i
 GROUP BY DATE_FORMAT(first_shoping,'%Y%m');

SET @flag:=NULL;
SELECT IFNULL(yearmonth,0) INTO @flag FROM sum_month_save_new_customer_quarter WHERE yearmonth=DATE_FORMAT(@sdate,'%Y%m') AND quarter_type=1;
IF(@flag=0)THEN
	insert into sum_month_save_new_customer_quarter
	  SELECT DATE_FORMAT(@sdate,'%Y%m'),1,IFNULL(@flag,0),IFNULL(@flag,0);
END IF;

SET @sdate:=(SELECT min(dim_date_id) FROM dim_date_day 
     where substr(dim_date_id,1,6)= DATE_FORMAT(date_sub(@date,interval 9 month),'%Y%m'));
SET @edate:=(SELECT max(dim_date_id) FROM dim_date_day 
     where substr(dim_date_id,1,6)= DATE_FORMAT(date_sub(@date,interval 9 month),'%Y%m'));
SET @sdate_i=DATE_FORMAT(DATE_ADD(@sdate,INTERVAL 6 MONTH),'%Y%m%d');
SET @edate_i=DATE_FORMAT(DATE_SUB(DATE_ADD(@sdate,INTERVAL 9 MONTH),INTERVAL 1 DAY),'%Y%m%d');

DROP  TABLE IF EXISTS t_member;
CREATE  TABLE IF NOT EXISTS t_member(  `dim_member_id` VARCHAR(32) DEFAULT '0',`first_shoping` DATE,KEY `IDX_member` (`dim_member_id`) USING BTREE);

INSERT INTO t_member
SELECT dim_member_id,first_shoping
  FROM dim_member a
  JOIN dim_shop b ON a.reg_shop_code=b.shop_code AND b.`online`=1
 WHERE first_shoping BETWEEN @sdate AND @edate
   AND dim_member_id<>0;

delete from sum_month_save_new_customer_quarter where yearmonth=DATE_FORMAT(@sdate,'%Y%m') AND quarter_type=2;
insert into sum_month_save_new_customer_quarter
SELECT DATE_FORMAT(first_shoping,'%Y%m') AS yearmonth,2 AS quarter_type,COUNT(DISTINCT a.dim_member_id),sum(amount)
  FROM t_member AS b
  JOIN fct_sales AS a ON a.dim_member_id=b.dim_member_id
 WHERE dim_date_id BETWEEN @sdate_i AND @edate_i
 GROUP BY DATE_FORMAT(first_shoping,'%Y%m');

SET @flag:=NULL;
SELECT IFNULL(yearmonth,0) INTO @flag FROM sum_month_save_new_customer_quarter WHERE yearmonth=DATE_FORMAT(@sdate,'%Y%m') AND quarter_type=2;
IF(@flag=0)THEN
	insert into sum_month_save_new_customer_quarter
	  SELECT DATE_FORMAT(@sdate,'%Y%m'),2,IFNULL(@flag,0),IFNULL(@flag,0);
END IF;

SET @sdate:=(SELECT min(dim_date_id) FROM dim_date_day 
     where substr(dim_date_id,1,6)= DATE_FORMAT(date_sub(@date,interval 12 month),'%Y%m'));
SET @edate:=(SELECT max(dim_date_id) FROM dim_date_day 
     where substr(dim_date_id,1,6)= DATE_FORMAT(date_sub(@date,interval 12 month),'%Y%m'));
SET @sdate_i=DATE_FORMAT(DATE_ADD(@sdate,INTERVAL 9 MONTH),'%Y%m%d');
SET @edate_i=DATE_FORMAT(DATE_SUB(DATE_ADD(@sdate,INTERVAL 12 MONTH),INTERVAL 1 DAY),'%Y%m%d');

DROP  TABLE IF EXISTS t_member;
CREATE  TABLE IF NOT EXISTS t_member(  `dim_member_id` VARCHAR(32) DEFAULT '0',`first_shoping` DATE,KEY `IDX_member` (`dim_member_id`) USING BTREE);

INSERT INTO t_member
SELECT dim_member_id,first_shoping
  FROM dim_member a
  JOIN dim_shop b ON a.reg_shop_code=b.shop_code AND b.`online`=1
 WHERE first_shoping BETWEEN @sdate AND @edate
   AND dim_member_id<>0;

delete from sum_month_save_new_customer_quarter where yearmonth=DATE_FORMAT(@sdate,'%Y%m') AND quarter_type=3;
insert into sum_month_save_new_customer_quarter
SELECT DATE_FORMAT(first_shoping,'%Y%m') AS yearmonth,3 AS quarter_type,COUNT(DISTINCT a.dim_member_id),sum(amount)
  FROM t_member AS b
  JOIN fct_sales AS a ON a.dim_member_id=b.dim_member_id
 WHERE dim_date_id BETWEEN @sdate_i AND @edate_i
 GROUP BY DATE_FORMAT(first_shoping,'%Y%m');


SET @flag:=NULL;
SELECT IFNULL(yearmonth,0) INTO @flag FROM sum_month_save_new_customer_quarter WHERE yearmonth=DATE_FORMAT(@sdate,'%Y%m') AND quarter_type=3;
IF(@flag=0)THEN
	insert into sum_month_save_new_customer_quarter
	  SELECT DATE_FORMAT(@sdate,'%Y%m'),3,IFNULL(@flag,0),IFNULL(@flag,0);
END IF;
END WHILE;
END