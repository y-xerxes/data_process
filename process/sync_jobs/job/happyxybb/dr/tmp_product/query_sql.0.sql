SELECT
a.id AS product_code,
a.bar_code AS barcode,
a.name AS product_name,
a.kind_id AS categoryIII_code,
c.kind_name AS categoryIII_name,
a.brand_id AS brand_code,
b.brand_name AS brand_name,
d.id AS categoryII_code,
d.kind_name AS categoryII_name,
e.id AS categoryI_code,
e.kind_name AS categoryI_name,
a.purchase_price/10000 in_price,
a.sales_price/10000 sales_price,
case a.member_price when 0 then a.sales_price/10000 else a.member_price/10000 end member_price,
a.status product_state,
a.create_date pos_created_at
FROM
goods_base AS a
left JOIN goods_brand AS b ON a.brand_id = b.id
left JOIN goods_kind AS c ON substr(a.kind_id,1,6) = c.id
left JOIN goods_kind AS d ON substr(a.kind_id,1,4) = d.id
left JOIN goods_kind AS e ON substr(a.kind_id,1,2) = e.id
where a.update_date >= date_sub(curdate(),interval 7 day)