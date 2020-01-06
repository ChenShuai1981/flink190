import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.log4j.{Level, Logger}

object SocketSourceAvroparquetSink {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.flink").setLevel(Level.ERROR)
    System.setProperty("HADOOP_USER_NAME", "root")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val hdfs_parquet_file_save_path = "/Users/chenshuai1/Downloads/parquetFiles"
    env.enableCheckpointing(1000)
    val port = 9999
    val source = env.socketTextStream("localhost", port)

    val wc: DataStream[WORD] = source
      .flatMap(_.split("\\s"))
      .filter(_ != null)
      .filter(!"".equalsIgnoreCase(_))
      .map(WORD(_, 1))
      .keyBy("word")
      .timeWindow(Time.seconds(3))
      .sum("count")

    /**
      * ParquetAvroWriters 这种方式保存的文件，spark.read.parquet 可以直接读取
      *
      * 也可以 完整的写入到 hdfs文件中去
      */
    val sink_parquet: StreamingFileSink[WORD] = StreamingFileSink
      .forBulkFormat(new Path(hdfs_parquet_file_save_path), ParquetAvroWriters.forReflectRecord(classOf[WORD]))
      .withBucketAssigner(new DateTimeBucketAssigner())
      .build()
    wc.addSink(sink_parquet).setParallelism(1)

    env.execute("SocketSourceAvroparquetSink")

  }

  case class WORD(word:String, count:Int)
}