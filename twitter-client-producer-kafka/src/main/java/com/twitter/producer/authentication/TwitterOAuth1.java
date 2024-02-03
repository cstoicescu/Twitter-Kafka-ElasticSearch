package com.twitter.producer.authentication;



import com.bonsai.cstoicescu.ConfigPropertiesValues;

import java.io.IOException;
import java.util.Properties;

public class TwitterOAuth1 {

    private  String user;
    private  String consumerKey;
    private  String consumerSecret;
    private  String accesToken;
    private  String secretToken;

    public TwitterOAuth1() throws IOException {
        ConfigPropertiesValues configPropertiesReader = new ConfigPropertiesValues();
        Properties properties = configPropertiesReader.getValues();

        this.user = properties.getProperty("twitter.user");
        this.consumerKey = properties.getProperty("twitter.consumerKey");
        this.consumerSecret = properties.getProperty("twitter.consumerSecret");
        this.accesToken = properties.getProperty("twitter.accessToken");
        this.secretToken = properties.getProperty("twitter.secretToken");
    }

    public String getConsumerKey() {
        return consumerKey;
    }

    public String getConsumerSecret() {
        return consumerSecret;
    }

    public String getAccesToken() {
        return accesToken;
    }

    public String getSecretToken() {
        return secretToken;
    }

    @Override
    public String toString() {
        return "UserConfig{" +
                "\nuser='" + user + '\'' +
                ",\nconsumerKey='" + consumerKey + '\'' +
                ",\nconsumerSecret='" + consumerSecret + '\'' +
                ",\naccesToken='" + accesToken + '\'' +
                ",\nsecretToken='" + secretToken + '\'' +
                '}';
    }
}
