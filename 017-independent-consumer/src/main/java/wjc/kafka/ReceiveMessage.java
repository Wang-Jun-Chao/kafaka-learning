package wjc.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2018-10-11 14:37
 **/
public class ReceiveMessage extends Base {
    private final static Logger logger = LoggerFactory.getLogger(ReceiveMessage.class);

    private final static KafkaConsumer<String, Customer> CONSUMER = new KafkaConsumer<>(KAFKA_PROPS);


    public static void main(String[] args) throws JsonProcessingException {

        // 向集群请求主题可用的分区。如果只打算读取特定分区，可以跳过这一步。
        List<PartitionInfo> partitionInfos = CONSUMER.partitionsFor(TOPIC);
        List<TopicPartition> partitions = new ArrayList<>();
        if (partitionInfos != null) {
            for (PartitionInfo partition : partitionInfos) {
                partitions.add(new TopicPartition(partition.topic(), partition.partition()));
            }
            // 知道需要哪些分区之后，调用assign()方位。
            CONSUMER.assign(partitions);

            while (true) {
                ConsumerRecords<String, Customer> records = CONSUMER.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, Customer> record : records) {
                    System.out.println(MAPPER.writerWithDefaultPrettyPrinter()
                            .writeValueAsString(record.value()));
                }
            }
        }
    }
}
