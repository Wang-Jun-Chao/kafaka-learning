package wjc.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2018-10-12 11:29
 **/
public class BananaPartitioner implements Partitioner {
    private final static Logger logger = LoggerFactory.getLogger(BananaPartitioner.class);

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public int partition(String topic,
                         Object key,
                         byte[] keyBytes,
                         Object value,
                         byte[] valueBytes,
                         Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        if ((keyBytes == null) || (!(key instanceof String))) {
            throw new InvalidRecordException("We expect all messages to have customer name as key");
        }

        // Banana总是被分配到最后一个分区
        if (((String) key).equals("Banana")) {
            return numPartitions;
        }

        // 其他记录被散列到其他分区
        return (Math.abs(Utils.murmur2(keyBytes)) % (numPartitions - 1));
    }

    @Override
    public void close() {
    }
}
