package wjc.kafka;

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
public class CustomerSerializer implements Serializer<Customer> {
    private final static Logger logger = LoggerFactory.getLogger(CustomerSerializer.class);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // do nothing
    }

    @Override
    public byte[] serialize(String topic, Customer data) {

        try {

            if (data == null) {
                return null;
            } else {
                byte[] serializedName;
                int stringSize;

                if (data.getName() != null) {
                    serializedName = data.getName().getBytes(StandardCharsets.UTF_8);
                    stringSize = serializedName.length;
                } else {
                    serializedName = new byte[0];
                    stringSize = 0;
                }
                ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + stringSize);
                buffer.putInt(data.getId());
                buffer.putInt(stringSize);
                buffer.put(serializedName);

                return buffer.array();
            }
        } catch (Exception e) {
            logger.error("Error when serializing Customer to byte[]. {}", e);
        }
        return new byte[0];
    }

    @Override
    public void close() {
        // do nothing
    }
}
