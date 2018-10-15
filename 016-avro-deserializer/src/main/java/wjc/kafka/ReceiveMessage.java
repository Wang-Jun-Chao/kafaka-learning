package wjc.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;

/**
 * TODO 反序列化不成功
 *
 * @author: wangjunchao(王俊超)
 * @time: 2018-10-11 14:37
 **/
public class ReceiveMessage extends Base {
    private final static Logger logger = LoggerFactory.getLogger(ReceiveMessage.class);

    private final static KafkaConsumer<String, ConsumerRecord<String, GenericRecord>> CONSUMER = new KafkaConsumer<>(KAFKA_PROPS);


    public static void main(String[] args) throws JsonProcessingException {

        CONSUMER.subscribe(Collections.singletonList(TOPIC));

        while (true) {
            ConsumerRecords<String, ConsumerRecord<String, GenericRecord>> records = CONSUMER.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, ConsumerRecord<String, GenericRecord>> record : records) {

                System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(record.value()));
            }
        }

    }


}
