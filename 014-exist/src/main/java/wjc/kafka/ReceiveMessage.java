package wjc.kafka;

import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2018-10-11 14:37
 **/
public class ReceiveMessage extends Base {
    private final static Logger logger = LoggerFactory.getLogger(ReceiveMessage.class);

    private static String                        waitTime;
    private static KafkaConsumer<String, String> consumer;

    public static void main(String[] args) {
        CircularFifoBuffer buffer = new CircularFifoBuffer(2);
        consumer = new KafkaConsumer<>(KAFKA_PROPS);

        final Thread mainThread = Thread.currentThread();

        // Registering a shutdown hook so we can exit cleanly
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.warn("Starting exit...");
                // Note that shutdownhook runs in a separate thread, so the only thing we can safely do to a consumer is wake it up
                consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });


        try {
            consumer.subscribe(Collections.singletonList(TOPIC));

            // looping until ctrl-c, the shutdown hook will cleanup on exit
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                System.out.println(System.currentTimeMillis() + "  --  waiting for data...");
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());

                    int sum = 0;

                    try {
                        int num = Integer.parseInt(record.value());
                        buffer.add(num);
                    } catch (NumberFormatException e) {
                        // just ignore strings
                    }

                    for (Object o : buffer) {
                        sum += (Integer) o;
                    }

                    if (buffer.size() > 0) {
                        logger.warn("Moving avg is: " + (sum / buffer.size()));
                    }
                }
                for (TopicPartition tp : consumer.assignment()) {
                    logger.warn("Committing offset at position:" + consumer.position(tp));
                }
                consumer.commitSync();
            }
        } catch (WakeupException e) {
            // ignore for shutdown
        } finally {
            consumer.close();
            logger.warn("Closed consumer and we are done");
        }
    }


}
