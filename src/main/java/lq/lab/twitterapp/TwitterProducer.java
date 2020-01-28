package lq.lab.twitterapp;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    public TwitterProducer() {

    }

    public static void main(String[] args) {
        new TwitterProducer().run();

    }

    public void run() {

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);

        //create a twitter client
        Client cli = createTwitterClient(msgQueue);
        cli.connect();

        // create kafka producer

        // loop send tweet to kafka
        // on a different thread, or multiple different threads....
        while (!cli.isDone()) {
            try {
                String msg = msgQueue.poll(5, TimeUnit.SECONDS);
                if(msg != null) {
                    logger.info("msg: "+msg);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                cli.stop();
            }

        }
        logger.info("App done");
    }

    String consumerKey="mCrBJDOZKIshhISBwi9ffXKUP";
    String consumerSecret="YTgZ8wB36DJ2ga5s0VONXOVjA2TU91K11ZZDiGfcGeD7Qa5uRE";
    String token="951498411026604039-P7H9bCWD9Ge6wIvU0uaLPGrU1AmIrvx";
    String secret="68cG59uAv8fgR7PIEU7euCeeSSxBHR1Nsw16SIiZNkWlM";

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {


        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("bitcoin");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        // Attempts to establish a connection.
        return hosebirdClient;

    }
}
