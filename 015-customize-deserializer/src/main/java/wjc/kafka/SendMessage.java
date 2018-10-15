package wjc.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2018-10-11 14:37
 **/
public class SendMessage extends Base {
    private final static Logger logger = LoggerFactory.getLogger(SendMessage.class);

    private final static KafkaProducer<String, Customer> PRODUCER = new KafkaProducer<>(KAFKA_PROPS);

    public static void main(String[] args) {


        Random random = new Random();


        try {

            while (true) {

                int id = random.nextInt();
                ProducerRecord<String, Customer> record = new ProducerRecord<>(
                        TOPIC, new Customer(id, "people: " + id));

                RecordMetadata metadata = PRODUCER.send(record).get();
                logger.info(metadata.toString());

                TimeUnit.SECONDS.sleep(1);
            }

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

    }
}
