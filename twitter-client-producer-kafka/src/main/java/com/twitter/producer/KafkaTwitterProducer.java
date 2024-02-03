package com.twitter.producer;

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
import com.twitter.producer.authentication.TwitterOAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class KafkaTwitterProducer {

    Logger logger =  LoggerFactory.getLogger(KafkaTwitterProducer.class.getName());

    public static void main(String[] args) throws IOException {

        new KafkaTwitterProducer().start();
    }

    private void start() throws IOException {

        logger.info("Twitter Producer Setup");

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        //create twitter client
        Client client = this.createTwitterClient(msgQueue);


        //establish connection
        client.connect();

        //create kafka producer
        KafkaProducer<String, String > kafkaProducer = this.createKafkaProducer();

        //send tweets to kafka
        while(!client.isDone()){
            String msg = null;
            try {
                 msg = msgQueue.poll(5000, TimeUnit.SECONDS);
            } catch (InterruptedException interruptedException) {
                logger.info("Message Interrupted " + interruptedException );
                client.stop();
            }
            if(msg !=null) {
                logger.info("New tweet: " + msg);
                kafkaProducer.send(new ProducerRecord<>("twitter-tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if( e!=null)
                            logger.error("Error: " + e);
                    }
                });
            }
        }

        Runtime.getRuntime().addShutdownHook( new Thread( () -> {
            logger.info("Stopping Application: ...");
            logger.info("Stoppind Twitter Client...");
            client.stop();
            logger.info("Closing Producer...");
            kafkaProducer.close();
        }));

        logger.info("End of Tweets");
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) throws IOException {

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("bitcoin", "Romania", "Workout");
        hosebirdEndpoint.trackTerms(terms);

        TwitterOAuth1 twitterOAuth1 = new TwitterOAuth1();

        logger.info(String.valueOf(twitterOAuth1));
        // sample code // generate the OAuth Access token first and then set it with twitter handle

//         These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(twitterOAuth1.getConsumerKey(), twitterOAuth1.getConsumerSecret(), twitterOAuth1.getAccesToken(), twitterOAuth1.getSecretToken());
//        Authentication hosebirdAuth = new OAuth1("secret", "secret", "secret", "secret");

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();


        return hosebirdClient;
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        Properties producerProperties = new Properties();

        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        producerProperties.setProperty(ProducerConfig.RETRIES_CONFIG, "20");
        producerProperties.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG,"500");    // 0.5 secunde
        producerProperties.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "120000"); // 2 minute
        producerProperties.put("enable.idempotence", true);

        // settings compression property  | gzip, lz4, snappy, latter 2 recommended

        producerProperties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        producerProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); // how much a producer will wait before sending a batch out
        producerProperties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));  // 32kb batch size

        KafkaProducer<String , String> kafkaProducer = new KafkaProducer<String, String>(producerProperties);

        return kafkaProducer;
    }
}
