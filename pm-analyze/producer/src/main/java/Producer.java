import com.linuxense.javadbf.DBFException;
import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import redis.clients.jedis.Jedis;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by Hanyue on 2015/11/21.
 */
public final class Producer {

    public final static String TOPIC = "WeiBo";

    /**
     * @param args
     */
    public static void main(String[] args) throws IOException {
        InputStream fs = new FileInputStream(args[0]);
        try {
            KafkaProducer producer = GetKafkaProducer();

            SendMessages(new DBFReader(fs), producer);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fs.close();
        }
    }

    /**
     * @return
     */
    private static KafkaProducer GetKafkaProducer() {
        Properties props = new Properties();
        // 配置kafka的端口
        props.put("bootstrap.servers", "manager1:9091,master2:9092");
        // 配置value的序列化类
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 配置key的序列化类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //request.required.acks
        //0, which means that the producer never waits for an acknowledgement from the broker (the same behavior as 0.7). This option provides the lowest latency but the weakest durability guarantees (some data will be lost when a server fails).
        //1, which means that the producer gets an acknowledgement after the leader replica has received the data. This option provides better durability as the client waits until the server acknowledges the request as successful (only messages that were written to the now-dead leader but not yet replicated will be lost).
        //-1, which means that the producer gets an acknowledgement after all in-sync replicas have received the data. This option provides the best durability, we guarantee that no messages will be lost as long as at least one in sync replica remains.
        props.put("acks", "-1");

        return new KafkaProducer(props);
    }

    /**
     * @param reader
     * @throws DBFException
     */
    private static void SendMessages(DBFReader reader, KafkaProducer producer) throws DBFException, InterruptedException {
        Jedis jedis = RedisUtil.getJedis();

        try {
            if (!jedis.hexists("weibo", "count")) jedis.hset("weibo", "count", "0");
            long lastCount = Long.parseLong(jedis.hget("weibo", "count"));

            // 这里暂时使用长整型
            long count = 0;
            Object[] rowValues;
            while ((rowValues = reader.nextRecord()) != null) {
                count++;
                if (count <= lastCount) continue;

                JSONObject record = new org.json.JSONObject();
                record.put("ID", rowValues[0].toString().trim());
                record.put("CATEGORY", rowValues[4].toString().trim());
                record.put("PHOTONUM", rowValues[6].toString().trim());
                record.put("PHONE", rowValues[14].toString().trim());
                record.put("ENTERPRISE", rowValues[20].toString().trim());
                record.put("XY", rowValues[22].toString().trim() + "," + rowValues[23].toString().trim());
                producer.send(new ProducerRecord(TOPIC, 4, rowValues[0].toString().trim(), record.toString()));

                System.out.println(count);
                jedis.hincrBy("weibo", "count", 1);
                Thread.sleep(500);
            }
        } finally {
            RedisUtil.returnResource(jedis);
        }
    }

    /**
     *
     */
    private static void ShowField(DBFReader reader) throws DBFException {
        int fieldsCount = reader.getFieldCount();
        StringBuilder titleSb = new StringBuilder();
        for (int i = 0; i < fieldsCount; i++) {
            DBFField field = reader.getField(i);
            titleSb.append(field.getName());
            titleSb.append(" , ");
        }
        System.out.println(titleSb.toString());
    }
}
