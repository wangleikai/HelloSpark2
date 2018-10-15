package KafkaDemo;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * @author WangLeiKai
 * 2018/10/15  16:55
 */
public class ProducerDemo {
    public static void main(String[] args) {
        Properties props = new Properties();

        props.put("metadata.broker.list", "hadoop-master:9092,hadoop-slave02:9092,hadoop-slave03:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        for (int i = 1001; i <= 1100; i++)
            producer.send(new KeyedMessage<String, String>("test", "test-msg" + i));
    }
}