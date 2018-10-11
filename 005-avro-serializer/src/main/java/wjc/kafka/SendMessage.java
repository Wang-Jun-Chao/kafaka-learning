package wjc.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2018-10-11 14:37
 **/
public class SendMessage {
    private final static Logger       logger     = LoggerFactory.getLogger(SendMessage.class);
    private final static ObjectMapper MAPPER     = new ObjectMapper();
    private static       Properties   kafkaProps = new Properties();
    private final static String       SCHEMA_URL = "{\n" +
            "    \"namespace\": \"wjc.kafka\",\n" +
            "    \"type\": \"record\",\n" +
            "    \"name\": \"Customer\",\n" +
            "    \"fields\": [\n" +
            "        {\"name\": \"id\", \"type\": \"long\"},\n" +
            "        {\"name\": \"name\",  \"type\": \"string\"}\n" +
            "    ]\n" +
            "}";


    public static void main(String[] args) {

        kafkaProps.put("bootstrap.servers", "10.1.177.96:9092");
        kafkaProps.put("key.serializer", KafkaAvroSerializer.class.getName());
        kafkaProps.put("value.serializer", KafkaAvroSerializer.class.getName());
        kafkaProps.put("schema.registry.url", SCHEMA_URL);
        KafkaProducer<String, Customer> producer = new KafkaProducer<>(kafkaProps);

        try {
            int count = 0;
            while (count < 10) {
                count++;
                Customer customer = CustomerGenerator.getNext();
                logger.info("Generated customer {}", customer);
                ProducerRecord<String, Customer> record = new ProducerRecord<>(
                        "CustomerCountry", "" + customer.getId(), customer);

                RecordMetadata metadata = producer.send(record).get();
                logger.info(metadata.toString());
                TimeUnit.SECONDS.sleep(1);
            }

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

    }
}
