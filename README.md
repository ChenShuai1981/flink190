# flink190
Examples about Apache Flink 1.9.0 for self learning purpose.


###
[Flink中延时调用设计与实现](https://mp.weixin.qq.com/s/dUbT7qV6RZdjh7gLDMp4gQ)
DetectDelayJob

###
[滑动窗口优化](https://github.com/TU-Berlin-DIMA/scotty-window-processor)
ScottyWindowJob

###
[计算实时热门商品](http://wuchong.me/blog/2018/11/07/use-flink-calculate-hot-items/)
HotItems

###
[Hbase维表关联：LRU策略 (**新增了异步mini batch访问hbase方式**)](https://mp.weixin.qq.com/s/8Pg_VMtNB6wtYRZRPTOrjg)
HbaseAsyncLRUJob

###
[如何做定时输出](https://mp.weixin.qq.com/s/VUEvvoHoupZMpxWQsEeHEA)
BatchIntervalSinkJob

###
[自定义metric监控流入量](https://mp.weixin.qq.com/s/QG-L1Wey3LpaN1aD_Li8aQ)
JobMetricsExample
ParseKafkaSourceMetricsJob

###
[Mysql维表关联：全量加载](https://mp.weixin.qq.com/s/Hr7fg3rh2YaTCLtfyuuRfA)
ScheduleMySQLSideFullLoadJob

###
[维表服务与Flink异步IO](https://mp.weixin.qq.com/s/oMIc1RWcg2ynW0l6uaqIRQ)
MySqlAsyncIOJob

###
[kafka维表关联：广播方式](https://mp.weixin.qq.com/s/WyR_sGu-JNCJFkm2fAZ-PA)
KafkaBroadcastRuleJob
CustomerPurchaseBehaviorTracker

###
[自定义异步查询](https://mp.weixin.qq.com/s/kTpE8tNOchJeTeS13SD1jg)
CustomizeAsyncFunctionJob

###
[基于 Flink 实现的商品实时推荐系统](https://mp.weixin.qq.com/s/pF8mr4AeUwWWpGEAKmJW2w)

###
[基于Kafka+Flink+Redis的电商大屏实时计算案例](https://mp.weixin.qq.com/s/BPzOBz7oTfn2_yW8tevEEw)
DashboardJob

###
[Watermark测试](https://blog.csdn.net/qq_22222499/article/details/94997611)
WatermarkTest

###
[Flink intervalJoin 使用与原理分析](https://mp.weixin.qq.com/s/Sfi_CvNw7-OJHU-NplFcjA)
IntervalJoinDemo

###
[Flink DataStream中CoGroup实现原理与三种 join 实现](https://mp.weixin.qq.com/s?__biz=MzU5MTc1NDUyOA==&mid=2247483962&idx=1&sn=cd6a1c3687188088e61bc309529295b8&chksm=fe2b6675c95cef63731588cb572dd7bcb921d5db7a4ecc8692d8466f98d0c35e78148284802a&token=1137454042&lang=zh_CN&scene=21#wechat_redirect)
CoGroupDemo

###
[Flink时间系统系列之ProcessFunction 使用分析](https://mp.weixin.qq.com/s/cqbbxc0xLpSnyQS7C6Fg0Q)
ProcessFunctionExample

###
[PurgingTrigger使用](http://blog.madhukaraphatak.com/introduction-to-flink-streaming-part-6/)
WindowAnatomy

But most of the cases we want to clear the records once window evaluates, rather than keeping forever. In those situations we need to use purge trigger with count trigger.
Purging trigger is a trigger which normally wraps the other triggers. Purging trigger is responsible for purging all the values which are passed to the trigger from the window once window evaluates.
简言之，使用PurgingTrigger之后，状态不再累积，而是一次性的。PurgingTrigger通常需要配合其他Trigger使用。

###
ContinuousEventTimeTrigger使用，基于事件时间持续周期性地触发
ContinuousEventTimeTriggerDemo

###
FavouriteColour & FavouriteColourTable
分别用stream api和table api实现对favorite colour统计

###
StudentScoreExample
使用ROW_NUMBER() OVER窗口进行统计 + TopN统计

###
[flink exactly-once系列之StreamingFileSink分析](https://mp.weixin.qq.com/s/4EtkNns-KAzEqL3GRMRLAg)
StreamingFileSinkExample

###
[flink自定义trigger-实现窗口随意输出](https://mp.weixin.qq.com/s/8wxiC1QLpCHvnWjTJGOhXw)
KafkaSourceTriggerTest

###
三流join
FlinkWindow

###
延时自动过期元素窗口（最近30分钟打电话次数)
CustomWindowExample

###
采用Data Stream API构建interval join
IntervalJoinExample

###
订单超时未运发出警告
TimeoutAlert