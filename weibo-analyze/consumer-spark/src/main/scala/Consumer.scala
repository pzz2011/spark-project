
import org.apache.commons.codec.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

object Consumer {

  val TOPIC: String = "WeiBo"

  def main(args: Array[String]) {
    //mvn clean scala:compile compile package
    val conf = new SparkConf().setMaster("spark://master1:9000").setAppName("WeiBo-Analyze")
    val ssc = new StreamingContext(conf, Duration(1000))
    val kafkaStream = {
      val sparkStreamingConsumerGroup = "spark-streaming-consumer-group"
      val kafkaParams = Map(
        "metadata.broker.list" -> "manager1:9091,master2:9092",
        "serializer.class" -> "kafka.serializer.StringEncoder",
        "zookeeper.connect" -> "worker1:2181,worker2:2181,worker3:2181",
        "group.id" -> "weiBo-spark-streaming",
        "zookeeper.connection.timeout.ms" -> "1000")
      val inputTopic = TOPIC
      val numPartitionsOfInputTopic = 4
      val streams = (1 to numPartitionsOfInputTopic) map { _ =>
        KafkaUtils.createStream[String,String,StringDecoder, StringDecoder](ssc, kafkaParams, Map(inputTopic -> 1), StorageLevel.MEMORY_ONLY_SER).map(_._2)
      }

      val unifiedStream = ssc.union(streams)
      val sparkProcessingParallelism = 1
      unifiedStream.repartition(sparkProcessingParallelism)
    }

    // 获取Json对象
    val events = kafkaStream.flatMap(line => {
      val data = new org.json.JSONObject(line)
      Some(data)
    }).cache()

    val jedis = RedisUtil.pool.getResource
    if (!jedis.hexists("weibo", "categoryCount")) jedis.hset("weibo", "categoryCount", "")
    if (!jedis.hexists("weibo", "categoryPhotoCount")) jedis.hset("weibo", "categoryPhotoCount", "")
    if (!jedis.hexists("weibo", "enterPriseCount")) jedis.hset("weibo", "enterPriseCount", "0")
    RedisUtil.pool.returnResource(jedis)

    // 位置类型数量汇总
    val categoryCount = events.filter(_.getBoolean("ENTERPRISE") == 0).map(x => (x.getString("CATEGORY"), 1)).reduceByKey(_ + _)
    categoryCount.foreachRDD(rdd => {
      val jedis = RedisUtil.pool.getResource
      val data = jedis.hget("weibo", "categoryCount")
      if (data.isEmpty) {
        jedis.hset("weibo", "categoryCount", new org.json.JSONObject(rdd.collect()).toString)
      }
      else {
        rdd.foreachPartition(partitionOfRecords => {
          partitionOfRecords.foreach(pair => {
            val newObj = new org.json.JSONObject(data)
            newObj.put(pair._1, newObj.getLong(pair._1) + pair._2)
            jedis.hset("weibo", "categoryCount", newObj.toString())
          })
        })
        RedisUtil.pool.returnResource(jedis)
      }
    })

    // 位置类型照片汇总
    val categoryPhotoCount = events.filter(_.getBoolean("ENTERPRISE") == 0).map(x => (x.getString("CATEGORY"), x.getInt("PHOTONUM"))).reduceByKey(_ + _)
    categoryPhotoCount.foreachRDD(rdd => {
      val jedis = RedisUtil.pool.getResource
      val data = jedis.hget("weibo", "categoryPhotoCount")
      if (data.isEmpty) {
        jedis.hset("weibo", "categoryPhotoCount", new org.json.JSONObject(rdd.collect()).toString)
      }
      else {
        rdd.foreachPartition(partitionOfRecords => {
          partitionOfRecords.foreach(pair => {
            val newObj = new org.json.JSONObject(data)
            newObj.put(pair._1, newObj.getLong(pair._1) + pair._2)
            jedis.hset("weibo", "categoryPhotoCount", newObj.toString())
          }
          )
        })
        RedisUtil.pool.returnResource(jedis)
      }
    })

    // 企业数汇总
    val enterPriseCount = events.filter(_.getBoolean("ENTERPRISE") == 1)
    enterPriseCount.foreachRDD(rdd => {
      val jedis = RedisUtil.pool.getResource
      jedis.hincrBy("weibo", "enterPriseCount", rdd.count())
      RedisUtil.pool.returnResource(jedis)
    })

    ssc.start()
    ssc.awaitTermination()
  }
}