# 描述

会员升级统计业务的支撑数据

## 模型

### 时间维度数据(member_referee_day)

|||
|------|------|
|存储类型|mysql|
|存储位置|3307.joowing_statistics|
|描述|会员升级统计业务查询按天汇总的数据|
|计算间隔|每日更新|

**模型描述**

|名称|类型|描述|
|----|----|----|
|org_code|varchar(32)|商户编号|
|dim_year_id|int(11)|年id|
|dim_month_id|int(11)|年月id|
|dim_date_id|int(11)|年月日id|
|register_num|int(11)|线上注册数量|
|bind_num|int(11)|线上绑定数量|
|total_referee_num|int(11)|总升级会员数|
|used_cp_num|int(11)|首登券用券数量|
|not_all_channel_member_num|int(11)|非全渠道会员数量|
|not_member_num|int(11)|非会员数量|
|total_register_num|int(11)|总注册数量|
|order_num|int(11)|线下小票数量|
|order_person_num|int(11)|线下购物人次|
|not_all_channel_num|int(11)|非全渠道人次|
|referee_rate|float(6,2)|升级率|
|online_register_rate|float(6,2)|线上注册率|
|use_cp_rate|float(6,2)|首登券用券率|
|bind_rate|float(6,2)|老客升级率|
|created_at|datetime|入表时间|
|updated_at|datetime|修改时间|

### 门店维度数据(member_referee_shop)

|||
|--------|-----|
|存储类型|mysql|
|存储位置|3307.joowing_statistics|
|描述|会员升级统计业务按门店查询的数据|
|计算间隔|每日更新|

**模型描述**

|名称|类型|描述|
|----|----|----|
|org_code|varchar(32)|商户编号|
|develop_shop_id|int(11)|门店编号|
|shop_name|varchar(64)|门店名称|
|dim_year_id|int(11)|年id|
|dim_month_id|int(11)|年月id|
|dim_date_id|int(11)|年月日id|
|register_num|int(11)|线上注册数量|
|bind_num|int(11)|线上绑定数量|
|total_referee_num|int(11)|总升级会员数|
|used_cp_num|int(11)|首登券用券数量|
|not_all_channel_member_num|int(11)|非全渠道会员数量|
|not_member_num|int(11)|非会员数量|
|total_register_num|int(11)|总注册数量|
|order_num|int(11)|线下小票数量|
|order_person_num|int(11)|线下购物人次|
|not_all_channel_num|int(11)|非全渠道人次|
|referee_rate|float(6,2)|升级率|
|online_register_rate|float(6,2)|线上注册率|
|use_cp_rate|float(6,2)|首登券用券率|
|bind_rate|float(6,2)|老客升级率|
|created_at|datetime|入表时间|
|updated_at|datetime|修改时间|

