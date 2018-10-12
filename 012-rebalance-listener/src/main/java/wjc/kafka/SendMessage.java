package wjc.kafka;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2018-10-11 14:37
 **/
public class SendMessage extends Base {
    private final static Logger logger = LoggerFactory.getLogger(SendMessage.class);

    public static void main(String[] args) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(KAFKA_PROPS);
        ProducerRecord<String, String> record = new ProducerRecord<>(
                TOPIC, "Precision Products");

        try {
            while (true) {
                Future<RecordMetadata> future = producer.send(record);
                logger.info("{}", future.get());
                TimeUnit.SECONDS.sleep(1);
            }

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }


}
