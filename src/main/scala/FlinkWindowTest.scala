import java.util
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.streaming.api.{TimeCharacteristic, environment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.triggers.{ProcessingTimeTrigger, Trigger}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object FlinkWindowTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val input = env.socketTextStream("localhost", 9001)
    input.flatMap(f => {
      f.split("\\W+")
    }).map(line =>(line ,1))
      .keyBy(0)
      .window(
        new WindowAssigner[Object,TimeWindow] {
          override def isEventTime = false
          override def getDefaultTrigger(env: environment.StreamExecutionEnvironment) = {
            ProcessingTimeTrigger.create()
          }
          override def assignWindows(element: Object, timestamp: Long, context: WindowAssigner.WindowAssignerContext) = {
            val windows = new util.ArrayList[TimeWindow](7)

            //每隔1分钟统计历史5分钟的数据
            val size =1000L * 60 * 5
            val slide = 1000L * 60
            val lastStart = timestamp - timestamp % slide
            var start = lastStart
            while (start > timestamp - size) {
              start -= slide
              windows.add(new TimeWindow(start, start + size))
            }

            //每隔1分钟统计历史1分钟的数据
            val size1 =1000L * 60
            val lastStart1 = timestamp - timestamp % slide
            println(timestamp % slide)
            var start1 = lastStart1
            while (start1 > timestamp - size1) {
              windows.add(new TimeWindow(start1, start1 + size1))
              start1 -= slide
            }

            windows
          }

          override def getWindowSerializer(executionConfig: ExecutionConfig) = new TimeWindow.Serializer
        })
      .sum(1)
      .print()
    env.execute()
  }

}