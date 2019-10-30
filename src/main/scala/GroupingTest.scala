import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

import scala.collection.mutable

// https://oracle-base.com/articles/misc/rollup-cube-grouping-functions-and-grouping-sets
object GroupingTest {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    // 构造销售数据
    val salesData = new mutable.MutableList[(String, String, String, String, Int)]
    salesData.+=(("springy", "3C", "phone", "huawei", 4000))
    salesData.+=(("springy", "3C", "phone", "xiaomi", 3000))
    salesData.+=(("springy", "3C", "computer", "mac", 10000))
    salesData.+=(("springy", "3C", "computer", "thinkpad", 8000))
    salesData.+=(("springy", "3C", "phone", "huawei", 4000))
    salesData.+=(("springy", "clothes", "shoes", "adidas", 300))
    salesData.+=(("springy", "clothes", "shoes", "lining", 400))
    salesData.+=(("springy", "clothes", "pants", "jackjones", 500))
    salesData.+=(("stephenson", "clothes", "shoes", "adivon", 200))
    salesData.+=(("stephenson", "clothes", "shoes", "nike", 300))
    salesData.+=(("stephenson", "clothes", "skirt", "nike", 300))
    salesData.+=(("stephenson", "clothes", "skirt", "adidas", 400))

    val sales = env
      .fromCollection(salesData)
      .toTable(tEnv, 'name, 'class, 'item, 'object, 'price)

    tEnv.registerTable("Sales", sales)

    var sqlQuery =
      """
        |SELECT
        |  name,
        |  class,
        |  item,
        |  sum(price) AS sum_price,
        |  GROUPING(name) AS g_name,
        |  GROUPING(class) AS g_class,
        |  GROUPING(item) AS g_item
        |FROM
        |  Sales
        |GROUP BY CUBE (name, class, item)
        |ORDER BY name, class, item
        |""".stripMargin

    tEnv.sqlQuery(sqlQuery).toDataSet[Row].print()
  }

}