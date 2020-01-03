import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

import scala.collection.JavaConversions._
import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.api.reactive.RedisReactiveCommands
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture, RichAsyncFunction}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.kafka.clients.consumer.ConsumerConfig
import reactor.core.publisher.Mono

/**
  * 补充 aid 字段
  *
  * redis中准备数据
  * hmset 1 aid 3 cid 1
  * hmset 2 aid 4 cid 2
  *
  * socket中输入
  * 1,clientId1,1,1571646006000
  * 2,clientId1,1,1571646006000
  *
  * 期望输出
  * AdData(3,1,clientId1,1,1571646006000)
  * AdData(4,2,clientId1,1,1571646006000)
  */
object LookupRedis extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

//  val kafkaConfig = new Properties();
//  kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//  kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test1");
//  val consumer = new FlinkKafkaConsumer[String]("topic1", new SimpleStringSchema(), kafkaConfig);
  val ds: DataStream[AdData] = env.socketTextStream("localhost", 9999)
//    .addSource(consumer)
    .map(x => {
      val a: Array[String] = x.split(",")
      AdData(0, a(0).toInt, a(1), a(2).toInt, a(3).toLong) //默认给0
    })

  val redisSide: AsyncFunction[AdData, AdData] = new RedisSide
  AsyncDataStream.unorderedWait(ds, redisSide, 5L, TimeUnit.SECONDS, 1000)
    .print()
  env.execute("LookupRedis")
}

case class AdData(aId: Int, tId: Int, clientId: String, actionType: Int, time: Long)

class RedisSide extends RichAsyncFunction[AdData, AdData] {

  private var redisClient: RedisClient = _

  private var connection: StatefulRedisConnection[String, String] = _

  private var async: RedisAsyncCommands[String, String] = _

  override def open(parameters: Configuration): Unit = {

    val redisUri = "redis://localhost"
    redisClient = RedisClient.create(redisUri)
    connection = redisClient.connect()
    async = connection.async()
  }


  override def asyncInvoke(input: AdData, resultFuture: ResultFuture[AdData]): Unit = {
    val tid = input.tId.toString

    async.hget(tid, "aid").thenAccept(new Consumer[String]() {
      override def accept(t: String): Unit = {

        if (t == null) {
          resultFuture.complete(java.util.Arrays.asList(input))
          return
        }
        val aid = t.toInt
        val newData = AdData(aid, input.tId, input.clientId, input.actionType, input.time)
        resultFuture.complete(java.util.Arrays.asList(newData))
      }
    })

  }
  //关闭资源
  override def close(): Unit = {
    if (connection != null) connection.close()
    if (redisClient != null) redisClient.shutdown()
  }

}