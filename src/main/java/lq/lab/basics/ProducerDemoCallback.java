package lq.lab.basics;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoCallback {

    public static void main(String[] args) {

        String BOOTSTRAP_SERVER = "127.0.0.1:9092";

        Logger logger = LoggerFactory.getLogger(ProducerDemoCallback.class);

        // creation properties

        Properties prop = new Properties();


        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // create producer

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(prop);

        // create producer reccord

        for (int i = 0;i<10; i++) {
            ProducerRecord<String,String> reccord =
                    new ProducerRecord<String,String>("first_topic","Hello world call back " + Integer.toString(i));

            // send data

            producer.send(reccord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // Execute every time a reccord is successfully send or an exception is thrown
                    if(e == null) {

                        logger.info("Received new metadata. \n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "Offset:" + recordMetadata.offset() + "\n" +
                                "Timestamp:" + recordMetadata.timestamp() + "\n"
                        );
                    } else {
                        logger.error("Error while producing",e);
                    }

                }
            });


        }


        producer.flush();
        producer.close();


    }
}
