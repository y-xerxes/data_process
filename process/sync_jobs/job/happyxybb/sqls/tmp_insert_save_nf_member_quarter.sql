DROP PROCEDURE IF EXISTS `tmp_insert_save_nf_member_quarter`;
CREATE PROCEDURE `tmp_insert_save_nf_member_quarter`()
BEGIN
SET @date:='2015-06-01';
CREATE TABLE IF NOT EXISTS `sum_month_save_nf_customer_quarter` (
  `yearmonth` int(6) DEFAULT NULL,
  `quarter_type` int(1) DEFAULT NULL,
  `new_nf_member_num` int(11) DEFAULT NULL,
  `save_num` int(11) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

WHILE @date<='2017-04-01' DO
SET @date:=DATE_ADD(@date,INTERVAL 1 MONTH);

set @count:=NULL;

SET @sdate:=(SELECT min(dim_date_id) FROM dim_date_day 
     where substr(dim_date_id,1,6)= DATE_FORMAT(date_sub(@date,interval 6 month),'%Y%m'));
SET @edate:=(SELECT max(dim_date_id) FROM dim_date_day 
     where substr(dim_date_id,1,6)= DATE_FORMAT(date_sub(@date,interval 6 month),'%Y%m'));
SET @sdate_i=DATE_FORMAT(DATE_ADD(@sdate,INTERVAL 3 MONTH),'%Y%m%d');
SET @edate_i=DATE_FORMAT(DATE_SUB(DATE_ADD(@sdate,INTERVAL 6 MONTH),INTERVAL 1 DAY),'%Y%m%d');
SET @sdate_ii=DATE_FORMAT(DATE_SUB(@sdate,INTERVAL 6 MONTH),'%Y%m%d');
SET @edate_ii=DATE_FORMAT(DATE_SUB(@sdate,INTERVAL 1 DAY),'%Y%m%d');

DROP  TABLE IF EXISTS t_member;
CREATE  TABLE IF NOT EXISTS t_member(  `dim_member_id` VARCHAR(32) DEFAULT '0',KEY `IDX_member` (`dim_member_id`) USING BTREE);
DROP  TABLE IF EXISTS t_member_i;
CREATE  TABLE IF NOT EXISTS t_member_i(  `dim_member_id` VARCHAR(32) DEFAULT '0',KEY `IDX_member` (`dim_member_id`) USING BTREE);

INSERT INTO t_member
SELECT dim_member_id
  FROM fct_sales AS a
  JOIN dim_shop AS b ON a.dim_shop_id=b.dim_shop_id AND b.`online`=1
 WHERE dim_date_id BETWEEN @sdate AND @edate
   AND std_categoryi_code='01'
   AND dim_member_id<>0
 GROUP BY dim_member_id
;

INSERT INTO t_member_i
SELECT DISTINCT a.dim_member_id FROM t_member AS a
  JOIN fct_sales AS b ON a.dim_member_id=b.dim_member_id AND b.dim_date_id BETWEEN @sdate_ii AND @edate_ii AND b.std_categoryi_code='01';

DELETE a FROM t_member AS a
  JOIN t_member_i AS b ON a.dim_member_id=b.dim_member_id;

delete from sum_month_save_nf_customer_quarter where yearmonth=DATE_FORMAT(@sdate,'%Y%m') AND quarter_type=1;
set @count = (SELECT COUNT(DISTINCT dim_member_id) new_nf_member_num   FROM t_member );

insert into sum_month_save_nf_customer_quarter
SELECT DATE_FORMAT(@sdate,'%Y%m') ,1,@count, COUNT(DISTINCT a.dim_member_id) 
  FROM fct_sales AS a
  JOIN t_member AS b ON a.dim_member_id=b.dim_member_id
 WHERE std_categoryi_code='01'
   AND dim_date_id BETWEEN @sdate_i AND @edate_i;

SET @flag:=NULL;
SELECT IFNULL(yearmonth,0) INTO @flag FROM sum_month_save_nf_customer_quarter WHERE yearmonth=DATE_FORMAT(@sdate,'%Y%m') AND quarter_type=1;
IF(@flag=0)THEN
	insert into sum_month_save_nf_customer_quarter
	  SELECT DATE_FORMAT(@sdate,'%Y%m'),1,IFNULL(@count,0),IFNULL(@flag,0);
END IF;

set @count:=NULL;

SET @sdate:=(SELECT min(dim_date_id) FROM dim_date_day 
     where substr(dim_date_id,1,6)= DATE_FORMAT(date_sub(@date,interval 9 month),'%Y%m'));
SET @edate:=(SELECT max(dim_date_id) FROM dim_date_day 
     where substr(dim_date_id,1,6)= DATE_FORMAT(date_sub(@date,interval 9 month),'%Y%m'));
SET @sdate_i=DATE_FORMAT(DATE_ADD(@sdate,INTERVAL 6 MONTH),'%Y%m%d');
SET @edate_i=DATE_FORMAT(DATE_SUB(DATE_ADD(@sdate,INTERVAL 9 MONTH),INTERVAL 1 DAY),'%Y%m%d');
SET @sdate_ii=DATE_FORMAT(DATE_SUB(@sdate,INTERVAL 6 MONTH),'%Y%m%d');
SET @edate_ii=DATE_FORMAT(DATE_SUB(@sdate,INTERVAL 1 DAY),'%Y%m%d');

DROP  TABLE IF EXISTS t_member;
CREATE  TABLE IF NOT EXISTS t_member(  `dim_member_id` VARCHAR(32) DEFAULT '0',KEY `IDX_member` (`dim_member_id`) USING BTREE);
DROP  TABLE IF EXISTS t_member_i;
CREATE  TABLE IF NOT EXISTS t_member_i(  `dim_member_id` VARCHAR(32) DEFAULT '0',KEY `IDX_member` (`dim_member_id`) USING BTREE);

INSERT INTO t_member
SELECT dim_member_id
  FROM fct_sales AS a
  JOIN dim_shop AS b ON a.dim_shop_id=b.dim_shop_id AND b.`online`=1
 WHERE dim_date_id BETWEEN @sdate AND @edate
   AND std_categoryi_code='01'
   AND dim_member_id<>0
 GROUP BY dim_member_id
;

INSERT INTO t_member_i
SELECT DISTINCT a.dim_member_id FROM t_member AS a
  JOIN fct_sales AS b ON a.dim_member_id=b.dim_member_id AND b.dim_date_id BETWEEN @sdate_ii AND @edate_ii AND b.std_categoryi_code='01';

DELETE a FROM t_member AS a
  JOIN t_member_i AS b ON a.dim_member_id=b.dim_member_id;

delete from sum_month_save_nf_customer_quarter where yearmonth=DATE_FORMAT(@sdate,'%Y%m') AND quarter_type=2;
set @count = (SELECT COUNT(DISTINCT dim_member_id) new_nf_member_num   FROM t_member );

insert into sum_month_save_nf_customer_quarter
SELECT DATE_FORMAT(@sdate,'%Y%m') ,2,@count, COUNT(DISTINCT a.dim_member_id) 
  FROM fct_sales AS a
  JOIN t_member AS b ON a.dim_member_id=b.dim_member_id
 WHERE std_categoryi_code='01'
   AND dim_date_id BETWEEN @sdate_i AND @edate_i;

SET @flag:=NULL;
SELECT IFNULL(yearmonth,0) INTO @flag FROM sum_month_save_nf_customer_quarter WHERE yearmonth=DATE_FORMAT(@sdate,'%Y%m') AND quarter_type=2;
IF(@flag=0)THEN
	insert into sum_month_save_nf_customer_quarter
	  SELECT DATE_FORMAT(@sdate,'%Y%m'),2,IFNULL(@count,0),IFNULL(@flag,0);
END IF;

set @count:=NULL;
SET @sdate:=(SELECT min(dim_date_id) FROM dim_date_day 
     where substr(dim_date_id,1,6)= DATE_FORMAT(date_sub(@date,interval 12 month),'%Y%m'));
SET @edate:=(SELECT max(dim_date_id) FROM dim_date_day 
     where substr(dim_date_id,1,6)= DATE_FORMAT(date_sub(@date,interval 12 month),'%Y%m'));
SET @sdate_i=DATE_FORMAT(DATE_ADD(@sdate,INTERVAL 9 MONTH),'%Y%m%d');
SET @edate_i=DATE_FORMAT(DATE_SUB(DATE_ADD(@sdate,INTERVAL 12 MONTH),INTERVAL 1 DAY),'%Y%m%d');
SET @sdate_ii=DATE_FORMAT(DATE_SUB(@sdate,INTERVAL 6 MONTH),'%Y%m%d');
SET @edate_ii=DATE_FORMAT(DATE_SUB(@sdate,INTERVAL 1 DAY),'%Y%m%d');

DROP  TABLE IF EXISTS t_member;
CREATE  TABLE IF NOT EXISTS t_member(  `dim_member_id` VARCHAR(32) DEFAULT '0',KEY `IDX_member` (`dim_member_id`) USING BTREE);
DROP  TABLE IF EXISTS t_member_i;
CREATE  TABLE IF NOT EXISTS t_member_i(  `dim_member_id` VARCHAR(32) DEFAULT '0',KEY `IDX_member` (`dim_member_id`) USING BTREE);

INSERT INTO t_member
SELECT dim_member_id
  FROM fct_sales AS a
  JOIN dim_shop AS b ON a.dim_shop_id=b.dim_shop_id AND b.`online`=1
 WHERE dim_date_id BETWEEN @sdate AND @edate
   AND std_categoryi_code='01'
   AND dim_member_id<>0
 GROUP BY dim_member_id
;

INSERT INTO t_member_i
SELECT DISTINCT a.dim_member_id FROM t_member AS a
  JOIN fct_sales AS b ON a.dim_member_id=b.dim_member_id AND b.dim_date_id BETWEEN @sdate_ii AND @edate_ii AND b.std_categoryi_code='01';

DELETE a FROM t_member AS a
  JOIN t_member_i AS b ON a.dim_member_id=b.dim_member_id;

delete from sum_month_save_nf_customer_quarter where yearmonth=DATE_FORMAT(@sdate,'%Y%m') AND quarter_type=3;
set @count = (SELECT COUNT(DISTINCT dim_member_id) new_nf_member_num   FROM t_member );

insert into sum_month_save_nf_customer_quarter
SELECT DATE_FORMAT(@sdate,'%Y%m') ,3,@count, COUNT(DISTINCT a.dim_member_id) 
  FROM fct_sales AS a
  JOIN t_member AS b ON a.dim_member_id=b.dim_member_id
 WHERE std_categoryi_code='01'
   AND dim_date_id BETWEEN @sdate_i AND @edate_i;

SET @flag:=NULL;
SELECT IFNULL(yearmonth,0) INTO @flag FROM sum_month_save_nf_customer_quarter WHERE yearmonth=DATE_FORMAT(@sdate,'%Y%m') AND quarter_type=3;
IF(@flag=0)THEN
	insert into sum_month_save_nf_customer_quarter
	  SELECT DATE_FORMAT(@sdate,'%Y%m'),3,IFNULL(@count,0),IFNULL(@flag,0);
END IF;

END WHILE;
END