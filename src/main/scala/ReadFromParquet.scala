import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.ParquetRowInputFormat
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.types.Row
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.{Level, Logger}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.schema.{MessageType, PrimitiveType}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition

object ReadFromParquet {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.flink").setLevel(Level.ERROR)
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val hdfs_parquet_file_path_t1 = "hdfs://ns1/user/hive/warehouse/test.db/t1"
    val hdfs_parquet_file_path = "hdfs://ns1//user/hhy/parquet/2019-11-18--10"


    /**
      * case 1: 手动指定 parquet的 schema
      */
    val id = new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT64, "id")
    val username = new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "username")
    val password = new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "password")
    val t1_schema = new MessageType("t1", id, username, password)
    println(s"t1_schema : ${t1_schema}")
    val t1 = env.readFile(new ParquetRowInputFormat(new Path(hdfs_parquet_file_path_t1), t1_schema), hdfs_parquet_file_path_t1)
    //print the second field
    t1.map(_.getField(2)).print().setParallelism(1)


    /**
      * case 2: 使用相关接口得到schema
      */
    val configurationconfiguration = new Configuration(true)
    val hdfs: FileSystem = org.apache.hadoop.fs.FileSystem.get(configurationconfiguration)
    val files = hdfs.listFiles(new org.apache.hadoop.fs.Path(hdfs_parquet_file_path), false)
    var flag = true
    var first_file_name = ""
    while (flag){
      if(files.hasNext){
        first_file_name = files.next().getPath.getName
        if(!first_file_name.equalsIgnoreCase(s"_SUCCESS") && !first_file_name.startsWith(".")){
          //          println(first_file_name)
          flag = false
        }
        //        println(s"flag:$flag")
      }else{
        flag = false
      }
    }

    println(s"first_file_name : ${first_file_name}")
    val parquetFileReader = ParquetFileReader.readFooter(configurationconfiguration, new org.apache.hadoop.fs.Path(hdfs_parquet_file_path + s"/${first_file_name}"))
    val schema: MessageType =parquetFileReader.getFileMetaData().getSchema()
    println(s"readed schema:${schema}")
    /**
      * using by read parquet file's schema
      */
    val t1_one: DataStream[Row] = env.readFile(new ParquetRowInputFormat(new Path(hdfs_parquet_file_path), schema), hdfs_parquet_file_path)

    t1_one.map(_.getField(1)).print().setParallelism(1)

    //执行job
    env.execute("ReadFromParquet")

  }
}