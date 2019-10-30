
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
  * With Csv Connector
  */
object TemporalTableJoinTest2 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = StreamTableEnvironment.create(env)
    env.setParallelism(1)

    val ordersSource = CsvTableSourceUtils.genRatesOrderSource
    val ratesSource = CsvTableSourceUtils.genRatesHistorySource

    tEnv.registerTableSource("Orders", ordersSource)
    tEnv.registerTableSource("RatesHistory", ratesSource)

    val tab = tEnv.scan("RatesHistory")
    // 创建TemporalTableFunction
    val temporalTableFunction = tab.createTemporalTableFunction('rowtime, 'currency)
    //注册TemporalTableFunction
    tEnv.registerFunction("Rates", temporalTableFunction)

    val SQLQuery =
      """
        |SELECT o.currency, o.amount, r.rate,
        |  o.amount * r.rate AS yen_amount
        |FROM
        |  Orders AS o,
        |  LATERAL TABLE (Rates(o.rowtime)) AS r
        |WHERE r.currency = o.currency
        |""".stripMargin

    tEnv.registerTable("TemporalJoinResult", tEnv.sqlQuery(SQLQuery))

    val result = tEnv.scan("TemporalJoinResult").toAppendStream[Row]
    // 打印查询结果
    result.print()
    env.execute()
  }

}
