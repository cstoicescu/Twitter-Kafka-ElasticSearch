package com.elastic.search.consumer;

import com.elastic.search.consumer.authentication.BonsaiAuth;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    public static void main(String[] args) throws IOException, InterruptedException {

        //create client
        RestHighLevelClient client = createClient();


        // create consumer
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer("twitter-tweets");

        // post kafka messages to elastic search
        while(true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

            Integer recordsCount = records.count();
            logger.info("Received " + records.count() + " records");

            BulkRequest bulkRequest = new BulkRequest();
            for(ConsumerRecord<String, String> record : records) {

                String tweeet = record.value();


                //creating unique id's
//                String id= record.topic() + record.partition() + record.offset();

                // use twitted unique id for each tweet

                String tweetIdentifier = extractIdFromTweet(record.value());

                IndexRequest indexRequest = new IndexRequest(
                        "twitter"
//                        "tweets",
//                        tweetIdentifier
                ).source(tweeet, XContentType.JSON);

                bulkRequest.add(indexRequest); //adding to bulk request


            }
            if( recordsCount > 0) {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Commiting offsets...");
                kafkaConsumer.commitSync();
                logger.info("Offsets have been committed");
                Thread.sleep(2000);
            }
        }
        //close the client
//        client.close();
    }

    private static JsonParser jsonParser = new JsonParser();

    private static String extractIdFromTweet(String tweetJson) {
      return  jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();

    }

    private static KafkaConsumer<String, String> createKafkaConsumer(String topic) {

        Properties properties = new Properties();

        //settings properties for consumer
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kafka-twitter-app");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }


    private static RestHighLevelClient createClient() {

        // doar pentru cloud setup
        BonsaiAuth bonsaiAuth;
        RestHighLevelClient restHighLevelClient = null;
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        try {
            bonsaiAuth = new BonsaiAuth();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(bonsaiAuth.getUsername(), bonsaiAuth.getPassword()));

            RestClientBuilder builder = RestClient.builder(
                    new HttpHost(bonsaiAuth.getHostname(), bonsaiAuth.getPort(), bonsaiAuth.getScheme()))
                    .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                        @Override
                        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                            return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        }
                    });
            restHighLevelClient = new RestHighLevelClient(builder);
        } catch (IOException e) {
            logger.error("Error occured while authenticating to elastic search cluster: " + e);
        }
        return restHighLevelClient;
    }
}
