import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.example.GroupWriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema._

object Consumer {

  val weiboSchema = new MessageType("WeiBo",
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT64, "ID"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "CATEGORY"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT64, "PHOTONUM"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "PHONE"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BOOLEAN, "ENTERPRISE"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.FLOAT, "X"),
    new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.FLOAT, "Y"))

  val writer = new ParquetWriter(new org.apache.hadoop.fs.Path("HDFS://weibo/weibo.parquet"), new GroupWriteSupport(), CompressionCodecName.SNAPPY, ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE)

  def main(args: Array[String]) {

    val kafkaConsumer: KafkaConsumer[String, String] = {
      val props: Properties = new Properties
      props.put("bootstrap.servers", "manager1:9092,master2:9092")
      props.put("group.id", "weibo-hdfs")
      props.put("enable.auto.commit", "true")
      props.put("auto.commit.interval.ms", "1000")
      props.put("session.timeout.ms", "30000")
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      new KafkaConsumer(props)
    }

    kafkaConsumer.subscribe("WeiBo")

    while (true) {
      val records = kafkaConsumer.poll(100).get("WeiBo")
      for (record: ConsumerRecord[String, String] <- records) {
        val data = new org.json.JSONObject(record.value())
        val addr = data.getString("XY").split(",")
        val group = new SimpleGroup(weiboSchema)
        group.add("ID", data.getLong("ID"))
        group.add("CATEGORY", data.getString("CATEGORY"))
        group.add("PHOTONUM", data.getInt("PHOTONUM"))
        group.add("PHONE", data.getString("PHONE"))
        group.add("ENTERPRISE", data.getBoolean("ENTERPRISE"))
        group.add("X", addr(0))
        group.add("Y", addr(1))
        writer.write(group)
      }
    }
    writer.close()
  }

  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      writer.close()
    }
  }
  sys.addShutdownHook(hook.run)
}