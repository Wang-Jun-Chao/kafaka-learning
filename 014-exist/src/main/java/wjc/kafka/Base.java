package wjc.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2018-10-12 15:57
 **/
public class Base {
    protected final static Properties   KAFKA_PROPS = new Properties();
    protected final static ObjectMapper MAPPER      = new ObjectMapper();
    protected final static String       TOPIC       = "customerCountries";

    static {
        KAFKA_PROPS.put("bootstrap.servers", "10.1.177.96:9092");
        KAFKA_PROPS.put("group.id", "CountryCounter");
        KAFKA_PROPS.put("key.serializer", StringSerializer.class.getName());
        KAFKA_PROPS.put("value.serializer", StringSerializer.class.getName());
        KAFKA_PROPS.put("key.deserializer", StringDeserializer.class.getName());
        KAFKA_PROPS.put("value.deserializer", StringDeserializer.class.getName());
    }
}
