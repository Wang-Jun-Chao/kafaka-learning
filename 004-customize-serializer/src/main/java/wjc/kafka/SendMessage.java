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
        kafkaProps.put("value.serializer", CustomerSerializer.class.getName());
        KafkaProducer<String, Customer> producer = new KafkaProducer<>(kafkaProps);

        ProducerRecord<String, Customer> record = new ProducerRecord<>(
                "CustomerCountry", new Customer(1, "one people"));

        try {
            RecordMetadata metadata = producer.send(record).get();
            logger.info(metadata.toString());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

    }
}
