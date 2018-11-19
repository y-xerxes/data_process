SELECT
     pm.plan_no as plan_no,
     pm.plan_name as plan_name,
     pm.plan_memo as plan_memo,
     pf.id as plan_item_no,
     pf.plan_name as plan_item_name,
     pf.plan_desc as plan_item_desc,
     pf.rule_no as rule_no,
     isnull(isnull(pm.confirm_date,pm.oper_date),pf.begin_date) as begin_date,
     isnull(pm.stop_date,pf.end_date) as end_date,
     case when approve_flag=0 then '0' when approve_flag=1 and GETDATE() BETWEEN ISNULL(pm.confirm_date,pm.oper_date) and ISNULL(pm.stop_date,pf.end_date) then '1' when approve_flag=2 or GETDATE()>ISNULL(pm.stop_date,pf.end_date) then '2' end as plan_status,
     pf.item_no as item_no,
     pb.branch_no as branch_no,
     isnull(pf.amt, 0) as amt,
     isnull(pf.ex_amt, 0) as ex_amt,
     isnull(pf.rate, 1.00) as rate,
     pm.multiple_flag as multiple_flag,
     pf.sale_price as sale_price,
     pf.price as price,
     case when pf.rule_no='DMI' then '1' else '0' end as need_combine
FROM
    t_rm_plan_master as pm
INNER JOIN
    t_rm_plan_flow as pf ON pm.plan_no=pf.plan_no
INNER JOIN
    (select plan_no,[branch_no]=stuff((select ',' + [branch_no] from t_rm_plan_branch as b where b.plan_no=a.plan_no for xml path('')),1,1,'') from t_rm_plan_branch a group by plan_no) as pb ON pm.plan_no=pb.plan_no
WHERE
    pf.rule_no in ('PSI','PQI','DDI','FRI','DMI','FMI','DQI')
    AND pm.vip_type in ('1','无') -- 不限定会员等级或限定普通会员
    AND pf.week='1111111' -- 每天促销
    AND pm.approve_flag=1 and GETDATE() BETWEEN ISNULL(pm.confirm_date,pm.oper_date) and ISNULL(pm.stop_date,pf.end_date)
    AND pf.limit is null