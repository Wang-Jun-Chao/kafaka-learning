package wjc.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.Random;
import java.util.UUID;

/**
 * @author: wangjunchao(王俊超)
 * @time: 2018-10-11 17:32
 **/
public class CustomerGenerator {

    public static Customer getNext() {
        return new Customer(System.currentTimeMillis(), UUID.randomUUID().toString());
    }
}
