package wjc.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2018-10-11 14:37
 **/
public class SendMessage {
    private final static Logger       logger     = LoggerFactory.getLogger(SendMessage.class);
    private final static ObjectMapper MAPPER     = new ObjectMapper();
    private static       Properties   kafkaProps = new Properties();


    public static void main(String[] args) {

        kafkaProps.put("bootstrap.servers", "10.1.177.96:9092");
        kafkaProps.put("key.serializer", StringSerializer.class.getName());
        kafkaProps.put("value.serializer", StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps);

        ProducerRecord<String, String> record = new ProducerRecord<>(
                "CustomerCountry", "Biomedical Materials", "USA");

        try {
            producer.send(record, new DemoProducerCallback());
            // 等待回调函数完成
            TimeUnit.SECONDS.sleep(3);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

    }
}
