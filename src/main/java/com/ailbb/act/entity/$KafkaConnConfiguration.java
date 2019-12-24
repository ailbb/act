package com.ailbb.act.entity;

import com.ailbb.ajj.$;
import com.ailbb.ajj.entity.$ConnConfiguration;

/**
 * Created by Wz on 8/21/2018.
 */
public class $KafkaConnConfiguration extends $ConnConfiguration {
    private int acks = 1;
    private int port = 9092;
    private String serializerKey = "org.apache.kafka.common.serialization.ByteArraySerializer";
    private String serializerValue = "org.apache.kafka.common.serialization.ByteArraySerializer";
    private String brokers;

    /*
    bootstrap.servers=localhost:39092
    request.required.acks=1
    key.serializer=org.apache.kafka.common.serialization.ByteArraySerializer
    value.serializer=org.apache.kafka.common.serialization.ByteArraySerializer
     */

    public $KafkaConnConfiguration(){}

    public $KafkaConnConfiguration(String brokers){ this.brokers = brokers; }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public $KafkaConnConfiguration setPort(int port) {
        this.port = port;
        return this;
    }

    public int getAcks() {
        return acks;
    }

    public $KafkaConnConfiguration setAcks(int acks) {
        this.acks = acks;
        return this;
    }

    public String getSerializerKey() {
        return serializerKey;
    }

    public $KafkaConnConfiguration setSerializerKey(String serializerKey) {
        this.serializerKey = serializerKey;
        return this;
    }

    public String getSerializerValue() {
        return serializerValue;
    }

    public $KafkaConnConfiguration setSerializerValue(String serializerValue) {
        this.serializerValue = serializerValue;
        return this;
    }

    public String getBrokers() {
        return !$.isEmptyOrNull(brokers) ? brokers : $.concat($.notNull(getIp()), ":", getPort());
    }

    public $KafkaConnConfiguration setBrokers(String brokers) {
        this.brokers = brokers;
        return this;
    }

    @Override
    public String toString() {
        return "$KafkaConnConfiguration{" +
                "acks=" + acks +
                ", port=" + port +
                ", serializerKey='" + serializerKey + '\'' +
                ", serializerValue='" + serializerValue + '\'' +
                ", brokers='" + brokers + '\'' +
                '}';
    }
}
