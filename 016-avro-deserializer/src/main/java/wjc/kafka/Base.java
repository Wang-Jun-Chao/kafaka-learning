package wjc.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.util.Properties;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2018-10-12 15:57
 **/
public class Base {
    protected final static Properties   KAFKA_PROPS       = new Properties();
    protected final static ObjectMapper MAPPER            = new ObjectMapper();
    protected final static String       TOPIC             = "customerCountries";
    protected final static String       SCHEMA_URL        = "http://10.1.177.96:8081";
    protected final static String       BOOTSTRAP_SERVERS = "10.1.177.96:9092";


    static {
        KAFKA_PROPS.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        KAFKA_PROPS.put("group.id", "CountryCounter");
        KAFKA_PROPS.put("key.serializer", KafkaAvroSerializer.class.getName());
        KAFKA_PROPS.put("key.deserializer", KafkaAvroDeserializer.class.getName());
        KAFKA_PROPS.put("value.serializer", KafkaAvroSerializer.class.getName());
        KAFKA_PROPS.put("value.deserializer", KafkaAvroDeserializer.class.getName());

        KAFKA_PROPS.put("schema.registry.url", SCHEMA_URL);
    }
}
