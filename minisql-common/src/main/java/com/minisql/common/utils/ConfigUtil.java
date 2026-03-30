package com.minisql.common.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置工具类
 */
public class ConfigUtil {

    private final Properties properties;

    public ConfigUtil() {
        this.properties = new Properties();
    }

    public ConfigUtil(String configFile) throws IOException {
        this.properties = new Properties();
        load(configFile);
    }

    public void load(String configFile) throws IOException {
        try (InputStream is = ConfigUtil.class.getClassLoader().getResourceAsStream(configFile)) {
            if (is != null) {
                properties.load(is);
            } else {
                try (FileInputStream fis = new FileInputStream(configFile)) {
                    properties.load(fis);
                }
            }
        }
    }

    public String getString(String key) {
        return properties.getProperty(key);
    }

    public String getString(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    public int getInt(String key) {
        return Integer.parseInt(properties.getProperty(key));
    }

    public int getInt(String key, int defaultValue) {
        String value = properties.getProperty(key);
        return value != null ? Integer.parseInt(value) : defaultValue;
    }

    public long getLong(String key) {
        return Long.parseLong(properties.getProperty(key));
    }

    public long getLong(String key, long defaultValue) {
        String value = properties.getProperty(key);
        return value != null ? Long.parseLong(value) : defaultValue;
    }

    public boolean getBoolean(String key) {
        return Boolean.parseBoolean(properties.getProperty(key));
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        String value = properties.getProperty(key);
        return value != null ? Boolean.parseBoolean(value) : defaultValue;
    }

    public String[] getStringArray(String key, String delimiter) {
        String value = properties.getProperty(key);
        return value != null ? value.split(delimiter) : new String[0];
    }

    public void set(String key, String value) {
        properties.setProperty(key, value);
    }

    public Properties getProperties() {
        return properties;
    }
}
