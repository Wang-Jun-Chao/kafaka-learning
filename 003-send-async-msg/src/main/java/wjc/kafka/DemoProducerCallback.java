package wjc.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2018-10-11 16:04
 **/
public class DemoProducerCallback implements Callback {
    private final static Logger logger = LoggerFactory.getLogger(DemoProducerCallback.class);

    @Override
    public void onCompletion(RecordMetadata metadata, Exception e) {
        logger.info("receive back: {}", metadata);
        if (e != null) {
            logger.error(e.getMessage(), e);
        }
    }
}
