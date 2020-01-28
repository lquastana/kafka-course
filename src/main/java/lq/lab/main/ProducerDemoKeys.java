package lq.lab.main;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        String BOOTSTRAP_SERVER = "127.0.0.1:9092";

        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        // creation properties

        Properties prop = new Properties();


        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // create producer

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(prop);

        // create producer reccord

        for (int i = 0;i<10; i++) {

            String topic = "first_topic";
            String value = "Hello world keys " + Integer.toString(i);
            String key = "id_"+ Integer.toString(i);

            ProducerRecord<String,String> reccord = new ProducerRecord<String,String>(topic,key,value);

            // send data

            logger.info("Key: "+key);

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
            }).get(); // block send to make it synchronous


        }


        producer.flush();
        producer.close();


    }
}
