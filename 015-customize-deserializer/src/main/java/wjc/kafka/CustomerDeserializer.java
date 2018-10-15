package wjc.kafka;

import com.sun.org.apache.regexp.internal.RE;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2018-10-11 16:59
 **/
public class CustomerDeserializer implements Deserializer<Customer> {
    private final static Logger logger = LoggerFactory.getLogger(CustomerDeserializer.class);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // do nothing
    }

    @Override
    public Customer deserialize(String topic, byte[] data) {

        int id;
        int nameSize;
        String name;

        try {
            if (data == null) {
                return null;
            } else if (data.length < 8) {
                throw new SerializationException("Size of data received by " +
                        "IntegerDeserializer is shorter than expected");
            }

            ByteBuffer buffer = ByteBuffer.wrap(data);
            id = buffer.getInt();
            nameSize = buffer.getInt();
            byte[] nameBytes = new byte[nameSize];
            buffer.get(nameBytes);

            name = new String(nameBytes, StandardCharsets.UTF_8);
            return new Customer(id, name);
        }catch (Exception e) {
            throw new SerializationException("Error when serializing Customer to byte[] " + e);
        }
    }

    @Override
    public void close() {
        // do nothing
    }
}
