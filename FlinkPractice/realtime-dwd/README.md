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

提取生成的评论表的数据，并将字典表中相关维度退化到评论表中，写出到 Kafka 对应的主题。