package org.apache.pig.backend.stratosphere.datastorage;

import java.util.Enumeration;
import java.util.Properties;
import eu.stratosphere.nephele.configuration.Configuration;

public class ConfigurationUtil {

    public static Configuration toConfiguration(Properties properties) {
        assert properties != null;
        final Configuration config = new Configuration();
        final Enumeration<Object> iter = properties.keys();
        while (iter.hasMoreElements()) {
            final String key = (String) iter.nextElement();
            final String val = properties.getProperty(key);
            config.setString(key, val);
        }
        return config;
    }
}
