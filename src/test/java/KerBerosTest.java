import com.ailbb.act.$;
import com.ailbb.act.entity.$ConfSite;
import com.ailbb.act.entity.$KerberosConnConfiguration;
import com.ailbb.act.kerberos.$Kerberos;
import com.ailbb.ajj.entity.$JDBCConnConfiguration;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by Wz on 9/10/2018.
 */
public class KerBerosTest {
    @Test
    public void test() throws Exception {
//        Assert.assertEquals("", KerBerosTest.init());
    }

    public static $Kerberos init(){
        $KerberosConnConfiguration kerberosConnConfiguration = new $KerberosConnConfiguration();
        $ConfSite confSite = new $ConfSite();

        kerberosConnConfiguration // berberos所在机器
                .setPrincipal("hive/db@CLOUDERA")
                .setKeyTab("/home/keytab/hive_db.keytab")

                .setIp("127.0.0.1")
                .setUsername("root")
                .setPassword("123456")
        ;

        confSite.setCoreSite("/home/core-site.xml")
                .setHbaseSite("/home/hbase-site.xml")
                .setHdfsSite("/home/hdfs-site.xml")
                .setHiveSite("/home/hive-site.xml")
                .setMapredSite("/home/mapred-site.xml")
                .setYarnSite("/home/yarn-site.xml");

        // 初始化
        return $.kerberos.init(kerberosConnConfiguration, confSite);
    }
}
