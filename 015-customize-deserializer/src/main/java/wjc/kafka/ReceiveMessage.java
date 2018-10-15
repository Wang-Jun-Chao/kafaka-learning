package wjc.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2018-10-11 14:37
 **/
public class ReceiveMessage extends Base {
    private final static Logger logger = LoggerFactory.getLogger(ReceiveMessage.class);

    private final static KafkaConsumer<String, Customer> CONSUMER = new KafkaConsumer<>(KAFKA_PROPS);


    public static void main(String[] args) throws JsonProcessingException {

       CONSUMER.subscribe(Collections.singletonList(TOPIC));

       while (true) {
           ConsumerRecords<String, Customer> records = CONSUMER.poll(Duration.ofMillis(100));
           for(ConsumerRecord<String, Customer> record: records) {
               System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(record.value()));
           }
       }

    }


}
