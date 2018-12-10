# 会员升级统计/新会员升级统计 整合工作

### 计划
1. 了解业务背景：1人/天
2. 了解前方关注哪些数据：0.5人/天
3. 制定方案（数据侧方案/研发侧方案）：2人/天
4. 建模：3人/天
5. 开发：5人/天
6. 测试：1/天
7. 文档及口径说明：0.5人/天

### 全渠道会员定义
1. 业务定义
    > 会员必须在线上有完整有效的渠道，使商户营销能够触及该会员，才算是全渠道会员。导购代开卡和被动开卡会员算有效会员，但是不算是全渠道会员。
2. 数据定义
    > 会员第一次只填手机号之后就会有first_active_time,填写完必填信息之后会有update_require_info_time,并且这个时间之后不会变更。导购代开卡只会完成第一步，被动开卡会员一步也不会走。

### 首登券定义
1. 只有全渠道会员才会发到首登券，在members表中根据update_required_info_time来判断
	1) 没有发展门店数据的会员不会有首登券，代开卡会员不会有首登券，完成完整注册流程、填完必填信息之后才会发到首登券。
	2) 商户可以配置首登券只能在线上或者线下或者两个渠道都可以使用。
2. 首登券的主业务全都是`会员升级`,`会员升级`主业务下只包含首登券的

### members同步DW.dim_member流程
> airflow的nr_joowingdw_daily_update_dim_table任务(20 4 * * * )，调用3306.joowingDW.Update_dim_member_online存储过程，取出members表中first_active_tiem>ydate的会员，将这些会员在DW.dim_member中set app_flag=1,app_reg_date=first_active_date

### 涉及存储过程
1. Update_dim_member_online (20 4 * * * )   -> dim_member
2. insert_shops_referee (31 2 * * * )   -> shops_referee(汇总门店每天注册绑定)
3. insert_newmember_used_cp_count (0 6 * * * )  -> newmember_used_cp_count
4. get_shop_referee_order (1 5 * * * )  -> shop_referee_order_member_count
5. insert_sum_day_new_member(2 10 * * * )   -> sum_day_new_member

### 新会员升级统计
1. 新注册会员数(register_count)
	1) 中间表:report_statistics.shops_referee,源表:ris_production.members
	2) 从members表中获取user_type为register的会员
2. 绑定会员数(bind_count)
	1) 中间表:report_statistics.shops_referee,源表:ris_production.members
	2) 从members表中获取user_type为bind的会员
3. 总升级会员数(total_count)
	1) total_count=register_count + bind_count
4. 首登券用券会员数(use_cp_newmember)
	1) 中间表:newmember_used_cp_count,源表:ris_production.members,pomelo_backend_production.history_coupon_histories
	2) 从券表找出当天商户用过券的会员，在会员表找出当天新注册的会员，根据时间join得出首登券用券会员
5. 非全渠道会员数(none_bind_member)
	1) 中间表:3306.joowingDW.shop_referee_order_member_count,源表:DW.(fct_sales,dim_member)
	2) sromc.member_num=(fct_sales.dim_member_id>0);srmoc.bind_member=count(fct_sales+dim_member.app_flag=1)
	3) none_bind_member=sromc.member_num - srmoc.bind_member
6. 到店非会员数(none_member_order)
	1) 中间表:3306.joowingDW.shop_referee_order_member_count,源表:DW.(fct_sales,dim_member)
	2) none_member_order=sromc.dis_reg_order=count(fct_sales.dim_member_id=0)
7. 总注册会员数(register_num)
	1) 中间表:sum_day_new_member,源表:DW.dim_member
	2) register_num=sdnm.new_member_num=count(normal_member=1)
8. 线上开卡率
	新注册会员数/总注册会员数
9. 首登券用券率
	使用首登券的会员/(新注册会员+绑定手机老会员)
10. 老客升级率
	绑定手机老会员/(绑定手机老会员+非全渠道会员)
11. 转换率
	(手机开卡新会员+绑定手机老会员)/(手机开卡新会员+绑定手机老会员+非会员数+非全渠道会员数)

### 会员升级统计
1. 手机开卡的新会员(register_count)
	1) 中间表:report_statistics.shops_referee,源表:ris_production.members
	2) 从members表中获取user_type为register的会员
2. 绑定手机的老会员(bind_count)
	1) 中间表:report_statistics.shops_referee,源表:ris_production.members
	2) 从members表中获取user_type为bind的会员
3. 会员升级总数(total_count)
    1) total_count=register_count + bind_count
4. 线下小票数(order_num)
    1) 中间表:3306.joowingDW.shop_referee_order_member_count,源表:DW.(fct_sales,dim_member)
    2) order_num=sromc.order_num=count(fct_sales.order_no)
5. 到店消费人次(to_be_num)
    1) 中间表:3306.joowingDW.shop_referee_order_member_count,源表:DW.(fct_sales,dim_member)
    2) sromc.member_num=(fct_sales.dim_member_id>0);sromc.dis_reg_order=count(fct_sales.dim_member_id=0)
    3) to_be_num=sromc.member_num + sromc.dis_reg_order
6. 到店非全渠道人数(to_be_num2)
    1) 中间表:3306.joowingDW.shop_referee_order_member_count,源表:DW.(fct_sales,dim_member)
    2) to_be_num2=to_be_num - sromc.bind_member
7. 转换率
	(手机开卡新会员+绑定手机老会员)/(手机开卡新会员+绑定手机老会员+非会员数+非全渠道会员数)

### 问题点
1. 明确全渠道会员定义
	1) 以往在统计全渠道会员时，都是用first_active_time来统计全渠道会员,然后在DW库中标记此会员的app_flag为1，但是没有走完整个注册流程的会员并不是全渠道会员，first_active_time仍然有数据。
	2) 应该用update_require_info_time来作为全渠道会员的标记。
2. 首登券用券会员数
	1) 现存逻辑以当天用过券的注册会员为使用首登券会员，因为之前在做的时候首登券还没有一个明确的定义，都是商户自己配置下发的券，会员注册当天使用的券就作为首登券。

	2) 现在需要再找技术支持确认以下有没有固定首登券的定义。
3. 首登券用券率
	1) 新注册会员中包含导购代开卡会员和被动注册会员，这些会员是没有发放首登券的，不应该包含在分母中。

	2) 可以拆分为发券率和用券率，统计新注册会员有多少会员发了首登券，发券会员中有多少会员使用了这个首登券。
4. 老客升级率
	1) 非全渠道会员是统计到店消费的非全渠道会员，但是绑定手机老会员不一定是到店消费时进行的绑定升级。

	2) 沿用现存逻辑，认定新绑定升级的老会员一定是被导购影响后升级的线下会员。
5. 转换率
	1) 非会员是统计到店消费的新会员，手机开卡新会员不一定是从非会员中转换而来的,绑定手机老会员不一定是非全渠道会员转换而来

	2) 沿用现存逻辑，认定新注册会员一定是受导购影响后注册的非会员。

# 会员升级统计/新会员升级统计 整合方案

### 全部字段
1. 商户
2. 门店名称
3. 时间
4. 手机开卡新会员
5. 绑定手机老会员
6. 总升级会员(汇总数据)
7. 首登券用券会员
8. 非全渠道会员数
9. 非会员数
10. 总注册会员数
11. 线下小票数(没有用到)
12. 到店消费人次(没有用到)
13. 到店非全渠道人次(汇总数据)
14. 转换率
15. 线上开卡率
16. 首登券用券率
17. 老客升级率

### 数据逻辑变动
1. 首登券用券会员
	1) 在history_coupon_histories表里面根据serial_no关联promotion_coupon_definitions，找出buz_id=3的券为首登券。
	2) 需要修改insert_newmember_used_cp_count的逻辑，不能用会员注册时间和用券时间相关联。在members中找出指定时间段内注册绑定的会员，在history_coupon_histories表中找出大于时间段起点的用券记录，限制券类型为首登券，获得结果即为首登券用券会员数。
	3) 如果会员是代开卡或者注册后没有登录，同样会发放首登券

2. 非全渠道会员数/到店非全渠道人数
	1) 修改joowingDW.update_dim_member_online的逻辑，将update_required_time>ydate的会员作为全渠道会员，在dim_member中的app_flag更新为1

3. insert_shops_referee(已更改)
	1) 券相关数据取基础业务为1的券插入shops_referee表中，应该改为主业务为3
	2) 会员表和券表关联后只取了注册绑定时间为指定时间当月的会员，应该取消这个限制

### 页面查询
1. 按店汇总(开始日期，结束日期，门店)
> 返回指定门店每家门店在指定时间段内汇总数据
2. 按天汇总(开始日期，结束日期)
> 返回指定时间段内每天所有门店的汇总数据
3. 按店查询(开始日期，结束日期，门店)
> 返回指定门店在指定时间段内每天的数据

### 页面展示字段
1. 按店汇总

|门店名称|线上注册会员|线上绑定会员|会员升级总计|首登券用券会员数|非全渠道会员数|非会员数|总注册会员数|转换率|线上开卡率|首登券用券率|老客升级率|
|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|

2. 按天汇总

|时间|线上注册会员|线上绑定会员|会员升级总计|首登券用券会员数|非全渠道会员数|非会员数|总注册会员数|转换率|线上开卡率|首登券用券率|老客升级率|
|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|

3. 按天查询

|时间|门店名称|线上注册会员|线上绑定会员|会员升级总计|首登券用券会员数|非全渠道会员数|非会员数|总注册会员数|转换率|线上开卡率|首登券用券率|老客升级率|
|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|-----|

### 需要计算的字段
1. 手机开卡新会员
2. 绑定手机老会员
3. 总升级会员
4. 首登券用券会员
5. 非全渠道会员数
6. 非会员数
7. 总注册会员数
8. 转换率
9. 线上开卡率
10. 登券用券率
11. 老客升级率

### 数据源表
1. ris_production.members
	> 1,2,3
2. ris_production.members,pomelo_backend_production.history_coupon_histories
    > 4
3. DW.(fct_sales,dim_member)
    > 5,6
4. DW.dim_member
    > 7
    
### 方案
原本的会员升级统计和非会员升级统计是通过两个存储过程，从4张汇总数据表中分别获取数据。现在要改为只有一张会员升级报表，这张会员升级报表包括时间和门店两个维度，每个维度只从一张表中获取数据。

