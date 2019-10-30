import java.sql.Timestamp

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

import scala.collection.mutable

object SimpleTimeIntervalJoin {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 构造订单数据
    val ordersData = new mutable.MutableList[(String, String, Timestamp)]
    ordersData.+=(("001", "iphone", new Timestamp(1545800002000L)))
    ordersData.+=(("002", "mac", new Timestamp(1545800003000L)))
    ordersData.+=(("003", "book", new Timestamp(1545800004000L)))
    ordersData.+=(("004", "cup", new Timestamp(1545800018000L)))

    // 构造付款表
    val paymentData = new mutable.MutableList[(String, String, Timestamp)]
    paymentData.+=(("001", "alipay", new Timestamp(1545803501000L)))
    paymentData.+=(("002", "card", new Timestamp(1545803602000L)))
    paymentData.+=(("003", "card", new Timestamp(1545803610000L)))
    paymentData.+=(("004", "alipay", new Timestamp(1545803611000L)))
    val orders = env
      .fromCollection(ordersData)
      .assignTimestampsAndWatermarks(new TimestampExtractor[String, String]())
      .toTable(tEnv, 'orderId, 'productName, 'orderTime.rowtime)
    val ratesHistory = env
      .fromCollection(paymentData)
      .assignTimestampsAndWatermarks(new TimestampExtractor[String, String]())
      .toTable(tEnv, 'orderId, 'payType, 'payTime.rowtime)

    tEnv.registerTable("Orders", orders)
    tEnv.registerTable("Payment", ratesHistory)

    var sqlQuery =
      """
        |SELECT
        |  o.orderId,
        |  o.productName,
        |  p.payType,
        |  o.orderTime,
        |  cast(payTime as timestamp) as payTime
        |FROM
        |  Orders AS o JOIN Payment AS p ON o.orderId = p.orderId AND
        | p.payTime BETWEEN orderTime AND orderTime + INTERVAL '1' HOUR
        |""".stripMargin
    tEnv.registerTable("TemporalJoinResult", tEnv.sqlQuery(sqlQuery))

    val result = tEnv.scan("TemporalJoinResult").toAppendStream[Row]
    result.print()
    env.execute()
  }

}

class TimestampExtractor[T1, T2]
  extends BoundedOutOfOrdernessTimestampExtractor[(T1, T2, Timestamp)](Time.seconds(10)) {
  override def extractTimestamp(element: (T1, T2, Timestamp)): Long = {
    element._3.getTime
  }
}