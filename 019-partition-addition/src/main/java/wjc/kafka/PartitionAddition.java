package wjc.kafka;

import kafka.admin.AdminUtils;
import kafka.admin.BrokerMetadata;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Int;
import scala.collection.mutable.ArraySeq;
import scala.collection.mutable.Seq;

import java.util.HashMap;
import java.util.Properties;
import java.util.UUID;


/**
 * @author: wangjunchao(王俊超)
 * @time: 2018-10-18 19:13
 **/
public class PartitionAddition {
    private final static Logger logger          = LoggerFactory.getLogger(PartitionAddition.class);
    /**
     * 连接ZK
     */
    private static final String ZK_CONNECT      = "10.1.177.73:2181";
    /**
     * session 过期时间
     */
    private static final int    SESSION_TIMEOUT = 30000;
    /**
     * 连接超时时间
     */
    private static final int    CONNECT_TIMEOUT = 30000;


    public static void createTopic(String topic, int partition, int replicate, Properties properties) {
        ZkUtils zkUtils = null;

        try {
            // 实例化ZkUtils
            zkUtils = ZkUtils.apply(ZK_CONNECT, SESSION_TIMEOUT, CONNECT_TIMEOUT,
                    JaasUtils.isZkSecurityEnabled());
            if (AdminUtils.topicExists(zkUtils, topic)) {
                AdminUtils.createTopic(zkUtils, topic, partition, replicate, properties, null);
                logger.warn("the topic [" + topic + "] has been created");

                // TODO 有问题
                AdminUtils.addPartitions(
                        zkUtils,
                        "partition-api-info",
                        null,
                        null,
                        2,
                        null,
                        true);
            } else {
                logger.warn("the topic [" + topic + "] is already exist");
                AdminUtils.deleteTopic(zkUtils, topic);
                logger.warn("the topic [" + topic + "] has been deleted");
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            zkUtils.close();
        }
    }

    public static void main(String[] args) {
        createTopic(UUID.randomUUID().toString().replace("-", ""), 1, 1, new Properties());
    }
}
