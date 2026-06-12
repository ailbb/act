import com.ailbb.act.$;
import com.ailbb.act.entity.$ConfSite;
import com.ailbb.act.entity.$KafkaConnConfiguration;
import com.ailbb.act.entity.$KerberosConnConfiguration;
import com.ailbb.act.hadoop.$Hadoop;
import com.ailbb.act.kafka.$Kafka;
import com.ailbb.act.kerberos.$Kerberos;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.security.PrivilegedExceptionAction;

/*
 * Created by Wz on 9/10/2018.
 */
public class KafkaTest {
    @Test
    public void test() throws Exception {
//        initKerberos();
//        initKafka();

//        $.kafka.send("topic", "hello");
    }

    public static $Kerberos initKerberos() {
        $KerberosConnConfiguration kerberosConnConfiguration = new $KerberosConnConfiguration();
        $ConfSite confSite = new $ConfSite();

        kerberosConnConfiguration // berberos所在机器
                .setPrincipal("hive/demo@DEMO_REALM")
                .setKeyTab("/demo/keytab/demo.keytab")

                .setIp("127.0.0.1")
                .setUsername("demo_user")
                .setPassword("demo_password")
        ;

        confSite.setCoreSite("/demo/conf/core-site.xml")
                .setHiveSite("/demo/conf/hive-site.xml")
                .setHdfsSite("/demo/conf/hdfs-site.xml")
                .setHbaseSite("/demo/conf/hbase-site.xml")
                .setSentrySite("/demo/conf/sentry-site.xml")
                .setYarnSite("/demo/conf/yarn-site.xml")
                .setMapredSite("/demo/conf/mapred-site.xml")
                .setSslClientSite("/demo/conf/ssl-client-site.xml");

        // 初始化
        return $.kerberos.init(kerberosConnConfiguration, confSite);
    }

    public static $Kafka initKafka() throws Exception {
        $KafkaConnConfiguration kafkaConnConfiguration = new $KafkaConnConfiguration();

        kafkaConnConfiguration.setIp("demo-kafka-host");

        return $.kafka.init(kafkaConnConfiguration);
    }
}
