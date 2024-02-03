package com.bonsai.cstoicescu;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigPropertiesValues {

    InputStream inputStream;

    public Properties getValues() throws IOException {
        Properties properties = new Properties();
        String configFile = "config.properties";
        try{

            inputStream = getClass().getClassLoader().getResourceAsStream(configFile);

            if(inputStream !=null)
                properties.load(inputStream);
            else
                throw new FileNotFoundException("property file " + configFile + " not found");
        } catch (Exception e) {
            System.out.println("Exception" + e);
        } finally {
            inputStream.close();
        }
        return properties;
    }
}
