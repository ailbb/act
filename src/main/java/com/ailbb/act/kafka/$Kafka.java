package com.ailbb.act.kafka;

import com.ailbb.act.entity.$KafkaConnConfiguration;
import com.ailbb.ajj.$;
import com.ailbb.ajj.entity.$Result;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by Wz on 8/28/2018.
 */
public class $Kafka {
    private Producer producer;
    public final int $PORT = 9092;

    public $Kafka init($KafkaConnConfiguration kafkaConnConfiguration){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", $.concat($.notNull(kafkaConnConfiguration.getIp()), ":", $.lastDef($PORT, kafkaConnConfiguration.getPort())));
        properties.setProperty("request.required.acks", $.str(kafkaConnConfiguration.getAcks()));
        properties.setProperty("key.serializer", kafkaConnConfiguration.getSerializerKey());
        properties.setProperty("value.serializer", kafkaConnConfiguration.getSerializerValue());

        return this.setProducer(new KafkaProducer<>(properties));
    }

    public $Result send(String topic, String value){
        $Result rs = $.result();
        ProducerRecord<String, Byte[]> record = new ProducerRecord(topic, $.str(value).getBytes());
        try {
            rs.setData(producer.send(record));
        } catch (Exception e) {
            rs.addError($.exception(e));
        }

        return rs;
    }

    public Producer getProducer() {
        return producer;
    }

    public $Kafka setProducer(Producer producer) {
        this.producer = producer;
        return this;
    }
}
