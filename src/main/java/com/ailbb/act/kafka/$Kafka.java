package com.ailbb.act.kafka;

import com.ailbb.act.entity.$KafkaConnConfiguration;
import com.ailbb.act.hadoop.$Hadoop;
import com.ailbb.ajj.$;
import com.ailbb.ajj.entity.$Result;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.security.PrivilegedExceptionAction;
import java.util.Properties;

/*
 * Created by Wz on 8/28/2018.
 */
public class $Kafka extends $Hadoop {
    private Producer producer;
    private $KafkaConnConfiguration kafkaConnConfiguration;

    public $Kafka init($KafkaConnConfiguration kafkaConnConfiguration){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaConnConfiguration.getBrokers());
        properties.setProperty("request.required.acks", $.str(kafkaConnConfiguration.getAcks()));
        properties.setProperty("key.serializer", kafkaConnConfiguration.getSerializerKey());
        properties.setProperty("value.serializer", kafkaConnConfiguration.getSerializerValue());

        this.kafkaConnConfiguration = kafkaConnConfiguration;
        return this.setProducer(new KafkaProducer<>(properties));
    }

    public $Result send(String topic, String value) throws Exception {
        return this.run(new PrivilegedExceptionAction<$Result>() {
            @Override
            public $Result run() throws Exception {
                $Result rs = $.result();
                ProducerRecord<String, Byte[]> record = new ProducerRecord(topic, $.str(value).getBytes());
                try {
                    rs.setData(producer.send(record));
                    $.info("Send Kafka {"+kafkaConnConfiguration.getBrokers()+"} Msg Topic: " + topic );
                } catch (Exception e) {
                    rs.addError($.exception(e));
                }

                return rs;
            }
        });
    }

    public Producer getProducer() {
        return producer;
    }

    public $Kafka setProducer(Producer producer) {
        this.producer = producer;
        return this;
    }
}
