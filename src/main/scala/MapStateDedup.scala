//import java.util.Properties
//
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
//import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction
//import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//import org.apache.flink.util.Collector
//import org.apache.kafka.clients.consumer.ConsumerConfig
//
//case class AdData(id:Int,devId:String,time:Long)
//
//case class AdKey(id:Int,time:Long)
//
//object MapStateDedup extends App {
//
//  val env = StreamExecutionEnvironment.getExecutionEnvironment
//  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//
////  val kafkaConfig = new Properties()
////  kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
////  kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test1")
////
////  val consumer = new FlinkKafkaConsumer[String]("local_ad_data", new SimpleStringSchema, kafkaConfig)
//
//  val ds = env.socketTextStream("localhost", 9999)
////    env.addSource[String](consumer)
//    .map(x=>{
//      val s=x.split(",")
//      AdData(s(0).toInt, s(1), s(2).toLong)
//    }).assignTimestampsAndWatermarks(
//    new BoundedOutOfOrdernessTimestampExtractor[AdData](Time.minutes(1)) {
//      override def extractTimestamp(element: AdData): Long = element.time
//    }).keyBy(x => {
//      val endTime = TimeWindow.getWindowStartWithOffset(x.time, 0,
//        Time.hours(1).toMilliseconds) + Time.hours(1).toMilliseconds
//      AdKey(x.id, endTime)
//    })
//
//  ds.process(new Distinct1ProcessFunction()).print()
//  env.execute()
//}
//
//
//class Distinct1ProcessFunction extends KeyedProcessFunction[AdKey, AdData, Void] {
//
//  var devIdState: MapState[String, Int] = _
//
//  var devIdStateDesc: MapStateDescriptor[String, Int] = _
//
//  var countState: ValueState[Long] = _
//
//  var countStateDesc: ValueStateDescriptor[Long] = _
//
//  override def open(parameters: Configuration): Unit = {
//    devIdStateDesc = new MapStateDescriptor[String, Int]("devIdState", TypeInformation.of(classOf[String]), TypeInformation.of(classOf[Int]))
//    devIdState = getRuntimeContext.getMapState(devIdStateDesc)
//
//    countStateDesc = new ValueStateDescriptor[Long]("countState", TypeInformation.of(classOf[Long]))
//    countState = getRuntimeContext.getState(countStateDesc)
//  }
//
//  override def processElement(value: AdData, ctx: KeyedProcessFunction[AdKey, AdData, Void]#Context, out: Collector[Void]): Unit = {
//    val currWatermark = ctx.timerService().currentWatermark()
//
//    if(ctx.getCurrentKey.time + 1 <= currWatermark) {
//      println("late data:" + value)
//      return
//    }
//
//    val devId = value.devId
//
//    devIdState.get(devId) match {
//      case 1 => {
//        //表示已经存在
//      }
//      case _ => {
//        //表示不存在
//        devIdState.put(devId, 1)
//        val c = countState.value()
//        countState.update(c + 1)
//        //还需要注册一个定时器
//        ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey.time + 1)
//      }
//    }
//    println(countState.value())
//  }
//
//  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[AdKey, AdData, Void]#OnTimerContext, out: Collector[Void]): Unit = {
//    println(timestamp + " exec clean~~~")
//    println(countState.value())
//    devIdState.clear()
//    countState.clear()
//  }
//
//}
