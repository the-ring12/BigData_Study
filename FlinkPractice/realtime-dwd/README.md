本模块实现 DWD 层的功能，会创建对应于各个域的子模块。

## realtime_dwd_base_log

对 Kafka 主题 topic-log 的数据类别处理转发到不同的主题，供下游处理，主要区分为：
- 错误日志
- 启动日志
- 曝光日志
- 动作日志
- 页面日志

此模块的功能包括：
1. 从 Kafka 主题中读取数据
2. ETL，将脏数据放到侧输出流
3. 对新老访客标记修复
4. 对数据分流并写到不同的 Kafka 主题

## realtime-dwd-interaction-comment-info

对于评论表（comment_info）中的数据，评论类别是一个 id，关联配置表（HBase 中的 dim_base_dic）确定评论的类别（好评、差评……）

## realtime-dwd-trade-cart-add

购物车数据表（cart_info)中过滤加购数据，insert 和 update(数量必须增加)

## realtime-dwd-trade-order-detail

交易域下单事务事实表

关联订单明细表、订单表、订单明细活动关联表、订单明细优惠券关联表四张事实业务表的insert操作，形成下单明细表，写入 Kafka 对应主题。

## realtime-dwd-trade-order-cancel-detail

取消订单事务事实

从 Kafka 读取topic_db主题数据，关联筛选订单明细表、取消订单数据、订单明细活动关联表、订单明细优惠券关联表四张事实业务表形成取消订单明细表，写入 Kafka 对应主题。

## realtime-dwd-trade-order-pay-suc-detail

交易支付成功事务事实

从 Kafka topic_db主题筛选支付成功数据、从dwd_trade_order_detail主题中读取订单事实数据、LookUp字典表，关联三张表形成支付成功宽表，写入 Kafka 支付成功主题。

## realtime-dwd-trade-order-refund

退单事务事实表

从 Kafka 读取业务数据，筛选退单表数据，筛选满足条件的订单表数据，建立 MySQL-Lookup 字典表，关联三张表获得退单明细宽表。

## realtime-dwd-trade-refund-pay-suc-detail

退款成功事务事实表

（1）从退款表中提取退款成功数据，并将字典表的dic_name维度退化到表中
（2）从订单表中提取退款成功订单数据
（3）从退单表中提取退款成功的明细数据