DROP PROCEDURE IF EXISTS `tmp_zhanbao_01`;
CREATE PROCEDURE `tmp_zhanbao_01`()
BEGIN
	
DROP TEMPORARY TABLE IF EXISTS tmp_table_01;
CREATE TEMPORARY TABLE tmp_table_01(shop_code VARCHAR(32), shop_name VARCHAR(32), quantity INT, amount FLOAT, KEY IDX_shop(shop_code));
INSERT INTO tmp_table_01(shop_code,shop_name, quantity, amount)
SELECT dx.shop_code,dx.shop_name,SUM(dx.quantity),SUM(dx.amount) FROM
(
	SELECT ds.shop_code shop_code,ds.shop_name  shop_name, SUM(quantity) quantity, SUM(amount) amount
		FROM fct_sales fs
		JOIN dim_product dp ON fs.dim_product_id=dp.dim_product_id
    JOIN dim_shop ds ON fs.dim_shop_id=ds.dim_shop_id
	 WHERE dim_date_id BETWEEN 20170501 AND 20170603
		 AND fs.amount<>0
		 AND dp.barcode IN ('8715845001816','8715845001823','8715845001830','8715845002721','8715845002738','8715845002745','8715845003070','8718868315281','8718868315298','8718868315304')
	GROUP BY fs.dim_shop_id
	UNION ALL
	SELECT fs.shop_code shop_code,sp.shop_name,   SUM(quantity)  quantity, SUM(amount)  amount
		FROM prt_sales_item fs  
    JOIN happyxybbdw.dim_shop AS sp on fs.shop_code=sp.shop_code
	 WHERE fs.date_id BETWEEN 20170501 AND 20170603
		 AND fs.amount<>0
		 AND fs.product_barcode IN ('8715845001816','8715845001823','8715845001830','8715845002721','8715845002738','8715845002745','8715845003070','8718868315281','8718868315298','8718868315304')
	GROUP BY fs.shop_code
) dx
GROUP BY dx.shop_code;

DROP TEMPORARY TABLE IF EXISTS tmp_table_02;
CREATE TEMPORARY TABLE tmp_table_02(shop_code VARCHAR(32), shop_name VARCHAR(32), quantity INT, amount FLOAT, KEY IDX_shop(shop_code));
INSERT INTO tmp_table_02(shop_code,shop_name, quantity, amount)
SELECT a.* FROM tmp_table_01 a
JOIN happyxybbdw.tmp_zhanbao_shop_target  b ON a.shop_code=b.shop_code
ORDER BY b.display_priority;

SELECT SUM(quantity),SUM(amount) INTO @q,@a FROM tmp_table_01;

INSERT INTO tmp_table_02 (shop_code,shop_name,quantity,amount)
SELECT '总计:','',@q,@a FROM prt_sales_item
LIMIT 1;

SELECT * FROM tmp_table_02 
UNION ALL
SELECT '销售截止时间:',MAX(sales_time),'','' FROM prt_sales_item;

END