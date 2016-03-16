import alluxio.AlluxioURI
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}
import redis.clients.jedis.JedisPool

import scala.util.parsing.json.JSONArray

/**
 * Getting data from Parquet File on Alluxio and Cluster Analysis(KMeans)
 *
 * Attention : With commandline (alluxio fs mount alluxio://host:port/weibo hdfs://weibo),
 * We can get Parquet File on Alluxio
 */
object ClusterAnalysis {
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("spark://master1:9000").setAppName("WeiBo-ClusterAnalyze")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val path = new AlluxioURI("/weibo/weibo.parquet")

    val data = sqlContext.read.parquet(path.getPath)
    val parsedData = data.map(s => Vectors.dense(s.getAs[Double]("X"), s.getAs[Double]("Y"))).cache()

    val clusters = {
      val numClusters = 5
      val numIterations = 20
      KMeans.train(parsedData, numClusters, numIterations)
    }

    val error = clusters.computeCost(parsedData)
    //println("Within Set Sum of Squared Errors = " + error)

    object RedisUtil extends Serializable {
      val redisHost = "manager1"
      val redisPort = 6379
      val redisTimeout = 30000
      lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)

      lazy val hook = new Thread {
        override def run = {
          println("Execute hook thread: " + this)
          pool.destroy()
        }
      }
      sys.addShutdownHook(hook.run)
    }

    val jedis = RedisUtil.pool.getResource
    jedis.hset("weibo", "computeCost", error.toString)
    jedis.hset("weibo", "clusterCenters", JSONArray(List(clusters.clusterCenters.map(_.toDense.toString()))).toString())
    RedisUtil.pool.returnResource(jedis)
  }
}


