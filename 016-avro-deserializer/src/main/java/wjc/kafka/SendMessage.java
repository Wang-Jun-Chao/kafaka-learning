package wjc.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2018-10-11 14:37
 **/
public class SendMessage extends Base{
    private final static Logger       logger     = LoggerFactory.getLogger(SendMessage.class);
    private final static String       SCHEMA_STRING = "{\n" +
            "    \"namespace\": \"wjc.kafka\",\n" +
            "    \"type\": \"record\",\n" +
            "    \"name\": \"Customer\",\n" +
            "    \"fields\": [\n" +
            "        {\"name\": \"id\", \"type\": \"long\"},\n" +
            "        {\"name\": \"name\",  \"type\": \"string\"}\n" +
            "    ]\n" +
            "}";


    public static void main(String[] args) {


        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(KAFKA_PROPS);
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(SCHEMA_STRING);


        try {
            while (true) {
                GenericRecord record = new GenericData.Record(schema);
                record.put("id", System.currentTimeMillis());
                record.put("name", UUID.randomUUID().toString());

                logger.info("Generated customer {}", record);
                ProducerRecord<String, GenericRecord> data = new ProducerRecord<>(
                        TOPIC, "" + record.get("id"), record);

                RecordMetadata metadata = producer.send(data).get();
                logger.info(metadata.toString());
                TimeUnit.SECONDS.sleep(1);
            }

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

    }
}
