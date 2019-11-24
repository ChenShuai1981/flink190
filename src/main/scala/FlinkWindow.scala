import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * 三流join
  */
object FlinkWindow {

  class MyTimeTimestampsAndWatermarks extends AssignerWithPeriodicWatermarks[(String,Int)] with Serializable{
    //生成时间戳
    val maxOutOfOrderness = 3500L // 3.5 seconds
    var currentMaxTimestamp: Long = _
    override def extractTimestamp(element: (String,Int), previousElementTimestamp: Long): Long = {
      val timestamp = System.currentTimeMillis()
      currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
      timestamp
    }
    override def getCurrentWatermark(): Watermark = {
      // return the watermark as current highest timestamp minus the out-of-orderness bound
      new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val input = env.socketTextStream("localhost", 9001)
    val inputMap = input.flatMap(f => {
      f.split("\\W+")
    }).map(line =>(line ,1)).assignTimestampsAndWatermarks(new MyTimeTimestampsAndWatermarks())
    inputMap.print()

    val input1 = env.socketTextStream("localhost", 9002)
    val inputMap1 = input1.flatMap(f => {
      f.split("\\W+")
    }).map(line =>(line ,1)).assignTimestampsAndWatermarks(new MyTimeTimestampsAndWatermarks())
    inputMap1.print()

    val input2 = env.socketTextStream("localhost", 9003)
    val inputMap2 = input2.flatMap(f => {
      f.split("\\W+")
    }).map(line =>(line ,1)).assignTimestampsAndWatermarks(new MyTimeTimestampsAndWatermarks())
    inputMap2.print()

    val aa = inputMap.join(inputMap1).where(_._1).equalTo(_._1).window(TumblingProcessingTimeWindows.of(Time.seconds(6)))
      .apply{(t1:(String,Int),t2:(String,Int), out : Collector[(String,Int,Int)])=>
        out.collect(t1._1,t1._2,t2._2)
      }
    aa.print()

    val cc = aa.join(inputMap2).where(_._1).equalTo(_._1).window(TumblingProcessingTimeWindows.of(Time.seconds(6)))
      .apply{(t1:(String,Int,Int),t2:(String,Int), out : Collector[(String,Int,Int,Int)])=>
        out.collect(t1._1,t1._2,t1._3,t2._2)
      }
    cc.print()

    env.execute()
  }
}