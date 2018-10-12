package wjc.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
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

    private static Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(KAFKA_PROPS);
        consumer.subscribe(Collections.singletonList(TOPIC));

        try {
            int count = 0;
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("topic = {}, partition = {}, offset = {}, customer = {}, country = {}",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());

                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, "no metadata"));
                    // 每1000个消息提交一次
                    if (count % 1000 == 0) {
                        consumer.commitAsync(currentOffsets, null);
                    }
                    count++;
                }
                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
                        if (e != null) {
                            logger.error(String.format("Commit failed for offsets %s", offsets), e);
                        }
                    }
                });

                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            consumer.commitSync();
        }

    }
}
