package com.elastic.search.consumer.authentication;

import com.bonsai.cstoicescu.ConfigPropertiesValues;

import java.io.IOException;
import java.util.Properties;

public class BonsaiAuth {

    private  String hostname;
    private  String username;
    private  String password;
    private  int port;
    private  String scheme;

    public int getPort() {
        return port;
    }

    public String getScheme() {
        return scheme;
    }

    public String getHostname() {
        return hostname;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public BonsaiAuth() throws IOException {
        ConfigPropertiesValues propertiesValues = new ConfigPropertiesValues();
        Properties properties = propertiesValues.getValues();

        this.hostname = properties.getProperty("bonsai.hostname");
        this.username = properties.getProperty("bonsai.username");
        this.password = properties.getProperty("bonsai.password");
        this.port = Integer.parseInt(properties.getProperty("bonsai.port"));
        this.scheme = properties.getProperty("bonsai.scheme");


    }
}
