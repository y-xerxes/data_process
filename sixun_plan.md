﻿﻿﻿﻿﻿﻿﻿﻿#思讯促销活动全对接

## 要求：
>根据提供信息还原一笔促销,只提供商品达到促销要求的价格信息取来的促销数据要求线上能够支持，品牌品类相关促销无法支持
## 字段：
>至少要有：商品、促销类型、金额、数量限制
## 思迅测试erp
> 1168358675
ma46v3
## sql
```sql
SELECT
     pm.plan_no as "促销编号",
     pm.plan_name as "方案名称",
     pm.plan_memo as "方案摘要",
     pf.id as "促销项编号",
     pf.plan_name as "促销项名称",
     pf.plan_desc as "促销项摘要",
     pf.rule_no as "促销类型",
     isnull(isnull(pm.confirm_date,pm.oper_date),pf.begin_date) as "开始日期",
     isnull(pm.stop_date,pf.end_date) as "结束日期",
         case when approve_flag=0 then '未审核' when approve_flag=1 and GETDATE() BETWEEN ISNULL(pm.confirm_date,pm.oper_date) and ISNULL(pm.stop_date,pf.end_date) then '已审核' when approve_flag=2 or GETDATE()>ISNULL(pm.stop_date,pf.end_date) then '已结束' end as '促销状态', 
     pf.item_no as "商品编号",
     pb.branch_no as "门店编号",
     isnull(pf.amt, 0) as "限制最低数量",
     isnull(pf.ex_amt, 0) as "满减金额",
     isnull(pf.rate, 1.00) as "折扣",
     pf.limit as "限制最高数量", 
     pm.multiple_flag, 
     pm.sect_dis, 
     pm.over_nodis,
     pf.sale_price as "售价", 
     pf.price as "促销价格"
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
    AND approve_flag=1 and GETDATE() BETWEEN ISNULL(pm.confirm_date,pm.oper_date) and ISNULL(pm.stop_date,pf.end_date)
```
## 信息：
### 可能的相关表：
    t_rm_saleflow
    t_bd_item_info
    t_rm_spec_price
    t_rm_plan_flow
    t_rm_plan_detail
    t_rm_plan_rule
    t_rm_plan_branch
    t_rm_plan_master
### 字段：
    t_rm_plan_rule.range_flag:A代表所有商品，I代表指定商品
    t_rm_plan_flow.branch_no是不全的,需要关联t_rm_plan_branch获取门店数据
    t_rm_plan_flow.vip_type:0:会员卡,1:金卡,2:钻石卡,3:游泳卡,(nb,y):内部员工
    t_rm_plan_master.notacc_flag: 0:积分，1：不积分
    t_rm_plan_master.approve_flag: 0:未审核 1:已审核 2:终止
### 类型:
####1. PSI:指定商品直接特价
>给定的price是准确的
>multiple_flag都是0，都不在会员价基础上打折
>amt,ex_amt,rate,limit为null,sale_price为销售价格,price为执行价
####2. PQI:指定商品买满多少个之后特价
>给定的price是准确的,指定商品只要满足指定数量，所有此商品都打折
>multiple_flag都是0，都不在会员价基础上打折
>amt为要求数量,ex_amt,rate,limit为null,sale_price为销售价格,price为执行价
####3. FRI:指定商品买满N元减M元
>multiple_flag: 1:按倍数减(每满N元就减M元)，0:不按倍数减
>amt为要求金额,ex_amt为满减金额,rate,limit为null,sale_price为销售价格,price为null
>t_rm_plan_master.pub_flag: 1:使用前台收银设置选项 0:本促销方案自行设置(orig_cond=1:前台以原价金额确认此促销是否起效,orig_compute=1:最终结算金额=原价金额-促销优惠)
####4. FMI:指定商品买满N个商品减M元
>multiple_flag: 1:按倍数减(每满N元就减M元)，0:不按倍数减
>amt为要求数量,ex_amt为满减金额,rate和limit为null,sale_price为销售价格,price为null
>t_rm_plan_master.pub_flag: 1:使用前台收银设置选项 0:本促销方案自行设置(orig_cond=1:前台以原价金额确认此促销是否起效,orig_compute=1:最终结算金额=原价金额-促销优惠)
####5. DDI:指定商品直接折扣
>amt,ex_amt,limit为null,rate为折扣,sale_price为销售价格,price为null
>t_rm_plan_master.multiple_flag: 1:在会员价基础上打折 0:不在会员价基础上打折
####6. DMI:指定商品任选多少件折扣
>默认超出部分也打折，选择数件数折(第一件7折第二件6折)后可以选择"超出部分不打折"
sect_dis: 1:数件数折 0:否
over_nodis: 1:超出部分不打折 0:超出部分打折
>amt为要求数量,ex_amt,limit为null,rate为折扣,sale_price为销售价格,price为null
####7. DQI:指定商品买满多少件打折
>默认超出部分也打折，选择数件数折(第一件7折第二件6折)后可以选择"超出部分不打折"
sect_dis: 1:数件数折 0:否
over_nodis: 1:超出部分不打折 0:超出部分打折
>amt为要求数量,ex_amt,limit为null,rate为折扣,sale_price为销售价格,price为null


### 需要确定的事项
1. pub_flag
2. 如何基于会员折扣打折
3. 库存
4. 互斥/共享
5. limit
6. 需要确定如果创建了未来的促销活动，现在是否可以审核

### 测试库连接方式
1. sql server
username:sa, password:1hblsqt, host:192.168.10.88, port:1433, database:issyytv3_yyplan
2. mysql
username:root, password:123456, host:192.168.10.50, port:3308

### 促销数据入库后处理
###### 处理项
1. 将线下促销活动的促销类型编号转换为线上促销类型编号
>在dw库中新建促销类型映射表，定义线上促销类型编号为std编号，为线下促销类型编号匹配std编号
2. 将相同折扣、相同满减条件的促销项，合并sku到同一个促销项(待确认)
3. 增删字段
    1) 删除pm.sect_dis
    2) 删除pm.over_nodis
    3) 增加need_combine
###### 新建表
1. rule_type
2. prt_plan
3. fct_plan
### 数据同步任务
创建plan任务，将商户侧促销活动数据抽取到tmp_plan，处理后存入fct_plan



### 数据同步方案
1. 在sync_jobs项目下的相应商户目录下创建/plan/tmp_plan，将任务上传到retailer_sync_jobs中。此任务将数据伪实时同步到prt_plan表中，同步语句需要做到增量同步。
2. 增量同步需要做到取到新开始的促销活动和刚刚结束的促销活动。新开始的促销活动一定有审核时间，可以记录目前dw库中最大审核时间，在商户数据库中取审核时间大于dw库中最大审核时间的促销活动，即新的促销活动。 这样的话，无法获知已经同步的促销活动是否已经结束。因此同步的语句一定要将dw库中正在进行中的促销活动也获取到，然后将这些促销活动的状态与线上做对比，如果已经结束，就将此促销活动在线上也结束。 或者，同步语句只同步正在进行中的促销活动，与线上做对比，如果线上没有，就是新的促销活动；如果线上的促销活动没有同步到，代表此促销活动已经结束。
3. 每次同步数据时先去dw库中获取正在进行的促销活动的最小时间，将商户数据库中大于此时间的所有促销活动同步过来，然后与dw库中的促销活动做对比。
4. 创建促销数据的dag，这个dag首先需要找出retailer_sync_jobs中所有的plan任务，然后将数据同步到dw库中。同步到dw库中后与源数据做比对，获取到新的促销活动以及已结束的促销活动，更新这些促销活动的类型编号，然后发信号给后台，将数据传到线上