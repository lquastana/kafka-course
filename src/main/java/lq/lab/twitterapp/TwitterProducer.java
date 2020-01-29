package lq.lab.twitterapp;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Properties;


public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    public TwitterProducer() {

    }

    public static void main(String[] args) {
        new TwitterProducer().run();

    }

    public void run() {

        // create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        //create a twitter client
        TwitterStream cli = createTwitterClient();

        cli.addListener(new StatusListener() {
            @Override
            public void onStatus(Status status) {

                String msgFull = "@" + status.getUser().getScreenName() + " - " + status.getText();
                System.out.println(msgFull);
                producer.send(new ProducerRecord<>("twitter_tweets", status.getUser().getScreenName(), status.getText()), new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("Something bad happened", e);
                        }
                    }
                });
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }

            @Override
            public void onStallWarning(StallWarning warning) {
                System.out.println("Got stall warning:" + warning);
            }

            @Override
            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        }).sample().filter(createFilter());
        logger.info("App done");
    }

    String consumerKey = "??";
    String consumerSecret = "??";
    String token = "??";
    String secret = "??";


    public TwitterStream createTwitterClient() {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey(consumerKey)
                .setOAuthConsumerSecret(consumerSecret)
                .setOAuthAccessToken(token)
                .setOAuthAccessTokenSecret(secret);
        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        return twitterStream;


    }

    private FilterQuery createFilter() {
        FilterQuery filterQuery = new FilterQuery();
        try {
            filterQuery.track("coronavirus");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new IllegalArgumentException(e);
        }
        return filterQuery;
    }

    public KafkaProducer<String, String> createKafkaProducer() {
        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create safe Producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // kafka 2.0 >= 1.1 so we can keep this as 5. Use 1 otherwise.

        // high throughput producer (at the expense of a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // 32 KB batch size

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }
}