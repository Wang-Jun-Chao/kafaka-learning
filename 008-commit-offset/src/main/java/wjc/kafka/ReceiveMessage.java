package wjc.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2018-10-11 14:37
 **/
public class ReceiveMessage {
    private final static Logger       logger     = LoggerFactory.getLogger(ReceiveMessage.class);
    private static       Properties   kafkaProps = new Properties();
    private final static ObjectMapper MAPPER     = new ObjectMapper();

    public static void main(String[] args) {

        kafkaProps.put("bootstrap.servers", "10.1.177.96:9092");
        kafkaProps.put("group.id", "CountryCounter");
        kafkaProps.put("key.deserializer", StringDeserializer.class.getName());
        kafkaProps.put("value.deserializer", StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps);

        consumer.subscribe(Collections.singletonList("customerCountries"));

        try {
            int count = 0;
            while (count < 10) {
                count++;
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("topic = {}, partition = {}, offset = {}, customer = {}, country = {}",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.key(),
                            record.value());
                }

                consumer.commitSync();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            consumer.close();
        }

    }
}
