package spark.stream.app

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory
import spark.util.KafkaZookeeperCheckPoint

object ZookeeperOffsetAPP {
  val logger = LoggerFactory.getLogger("ZookeeperOffsetAPP")



  def main(args: Array[String]): Unit = {
    // 创建SparkStreaming入口
    val conf = new SparkConf().setAppName("ZookeeperOffsetAPP")
      .setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(1))
    val checkPointPath = "checkpointDirectory"
    // val ssc = StreamingContext.getActiveOrCreate(checkPointPath, functionToCreateContext)
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.1.26:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("test")
    val stream = KafkaZookeeperCheckPoint.createMyZookeeperDirectKafkaStream(
      ssc,
      kafkaParams,
      topics,
      "spark"
    )


    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
      KafkaZookeeperCheckPoint.storeOffsets(offsetRanges, "spark")
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
