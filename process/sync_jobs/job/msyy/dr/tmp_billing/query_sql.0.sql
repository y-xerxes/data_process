SELECT
	order_no,
	sum( pay_total ) as real_amount
FROM
	order_history_logs
WHERE
    org_code='msyy'
    and trade_date > DATE_SUB(CURDATE(), INTERVAL 7 DAY)
	AND trade_date < CURDATE()
GROUP BY
	order_no