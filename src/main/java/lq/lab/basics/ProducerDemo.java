package lq.lab.basics;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {




    public static void main(String[] args) {

        String BOOTSTRAP_SERVER = "127.0.0.1:9092";

        // creation properties

        Properties prop = new Properties();


        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // create producer

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(prop);


            // create producer reccord

            ProducerRecord<String,String> reccord =
                    new ProducerRecord<String,String>("first_topic","Hello world" );

            // send data

            producer.send(reccord);





        producer.flush();
        producer.close();


    }
}
