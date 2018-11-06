DROP PROCEDURE IF EXISTS `tmp_zhanbao`;
CREATE PROCEDURE `tmp_zhanbao`()
BEGIN
	
DROP TEMPORARY TABLE IF EXISTS tmp_table;
CREATE TEMPORARY TABLE tmp_table(shop_code VARCHAR(32), shop_name VARCHAR(32), quantity INT, amount FLOAT, KEY IDX_shop(shop_code));

INSERT INTO tmp_table(shop_code, quantity, amount)
SELECT dx.shop_code,SUM(dx.quantity),SUM(dx.amount) FROM
(
	SELECT ds.shop_code shop_code, SUM(quantity) quantity, SUM(amount) amount
		FROM fct_sales fs
		JOIN dim_product dp ON fs.dim_product_id=dp.dim_product_id
    JOIN dim_shop ds ON fs.dim_shop_id=ds.dim_shop_id
	 WHERE dim_date_id BETWEEN 20170501 AND 20170603
		 AND fs.amount<>0
		 AND dp.barcode IN ('8715845001816','8715845001823','8715845001830','8715845002721','8715845002738','8715845002745','8715845003070','8718868315281','8718868315298','8718868315304')
	GROUP BY fs.dim_shop_id
	UNION ALL
	SELECT fs.shop_code shop_code,   SUM(quantity)  quantity, SUM(amount)  amount
		FROM prt_sales_item fs
	 WHERE date_id BETWEEN 20170501 AND 20170603
		 AND fs.amount<>0
		 AND product_barcode IN ('8715845001816','8715845001823','8715845001830','8715845002721','8715845002738','8715845002745','8715845003070','8718868315281','8718868315298','8718868315304')
	GROUP BY fs.shop_code
) dx
GROUP BY dx.shop_code;

SELECT * FROM
(SELECT dx.set_name AS '军团名称', tt AS '军团目标', tq AS '当前销售量', sc AS '时间进度', CONCAT(FORMAT(wc*100,2),'%') AS '完成进度'
  FROM
(
	SELECT tz.set_name, SUM(tz.target2) AS tt, SUM(tt.quantity) AS tq, CONCAT(FORMAT(DATEDIFF(CURDATE(),'2017-04-30')/(DATEDIFF('2017-06-03','2017-04-30'))*100,2),'%') AS sc, SUM(tt.quantity)/SUM(tz.target2) AS wc
		FROM tmp_table tt
		JOIN tmp_zhanbao_shop_target tz ON tt.shop_code=tz.shop_code
	GROUP BY tz.set_no
) dx
ORDER BY dx.wc DESC
) dy
UNION ALL 
SELECT '','销售截止时间:',DATE_FORMAT(MAX(sales_time),'%Y-%m-%d'),DATE_FORMAT(MAX(sales_time),'%T'),'' FROM prt_sales_item;

END