import com.ailbb.act.$;
import com.ailbb.act.entity.$ConfSite;
import com.ailbb.act.entity.$KafkaConnConfiguration;
import com.ailbb.act.entity.$KerberosConnConfiguration;
import com.ailbb.act.hadoop.$Hadoop;
import com.ailbb.act.kafka.$Kafka;
import com.ailbb.act.kerberos.$Kerberos;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Test;

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
                .setPrincipal("hive/db@CLOUDERA")
                .setKeyTab("/keytab/hive_db.keytab")

                .setIp("127.0.0.1")
                .setUsername("root")
                .setPassword("123456")
        ;

        confSite.setCoreSite("/broadtech/core-site.xml")
                .setHiveSite("/broadtech/hive-site.xml")
                .setHdfsSite("/broadtech/hdfs-site.xml")
                .setHbaseSite("/broadtech/hbase-site.xml")
                .setSentrySite("/broadtech/sentry-site.xml")
                .setYarnSite("/broadtech/yarn-site.xml")
                .setMapredSite("/broadtech/mapred-site.xml")
                .setSslClientSite("/broadtech/ssl-client-site.xml");

        // 初始化
        return $.kerberos.init(kerberosConnConfiguration, confSite);
    }

    public static $Kafka initKafka() throws Exception {
        $KafkaConnConfiguration kafkaConnConfiguration = new $KafkaConnConfiguration();

        kafkaConnConfiguration.setIp("slave08");

        return $.kafka.init(kafkaConnConfiguration);
    }
}
