package lq.lab.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        String BOOTSTRAP_SERVER = "127.0.0.1:9092";
        String groupId = "my-5-app";
        String topic = "first_topic";

        // create consumer config
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create conssumer

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);
        consumer.subscribe(Arrays.asList(topic));

        // poll for new data

        while(true) {
            ConsumerRecords<String,String> reccords =
                    consumer.poll(Duration.ofMillis(100)); // new in kafka 2.0.0

            for(ConsumerRecord reccord : reccords) {
                logger.info("---------------");
                logger.info("Key: "+reccord.key());
                logger.info("Value: "+reccord.value());
                logger.info("Partition: "+reccord.partition());
                logger.info("Offset: "+reccord.offset());
                logger.info("---------------");

            }


        }

    }
}
