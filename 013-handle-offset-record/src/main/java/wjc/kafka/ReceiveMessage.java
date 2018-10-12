package wjc.kafka;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
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

    private final static KafkaConsumer<String, String> consumer = new KafkaConsumer<>(KAFKA_PROPS);

    private final static Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public static void main(String[] args) {

        consumer.subscribe(Collections.singletonList(TOPIC), new SaveOffsetsOnRebalance(consumer));
        consumer.poll(Duration.ofMillis(0));

        for (TopicPartition partition: consumer.assignment()) {
            consumer.seek(partition, getOffsetFromDb(partition));
        }

        try {
            int count = 0;
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("topic = {}, partition = {}, offset = {}, customer = {}, country = {}",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    processRecord(record);
                    storeRecordInDb(record);
                    storeRecordInDb(record.topic(), record.partition(), record.offset());
                }

                commitDbTransaction();

                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            consumer.commitSync();
        }

    }

    private static void commitDbTransaction() {

    }

    private static void storeRecordInDb(ConsumerRecord<String, String> record) {

    }


    private static void storeRecordInDb(String topic, int partition, long offset) {

    }

    private static void processRecord(ConsumerRecord<String, String> record) {

    }

    private static long getOffsetFromDb(TopicPartition partition) {
        return 0;
    }

    public static class SaveOffsetsOnRebalance implements ConsumerRebalanceListener {
        private final Logger                        logger = LoggerFactory.getLogger(SaveOffsetsOnRebalance.class);
        private final KafkaConsumer<String, String> consumer;

        public SaveOffsetsOnRebalance(KafkaConsumer<String, String> consumer) {
            this.consumer = consumer;
        }


        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            commitDbTransaction();
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            for (TopicPartition partition: partitions) {
                consumer.seek(partition, getOffsetFromDb(partition));
            }
        }
    }
}
