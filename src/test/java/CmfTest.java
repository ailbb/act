import com.ailbb.act.$;
import com.ailbb.act.cmf.$CMF;
import com.ailbb.act.hbase.$HBase;
import com.ailbb.ajj.entity.$ConnConfiguration;
import org.junit.Assert;
import org.junit.Test;

/*
 * Created by Wz on 9/10/2018.
 */
public class CmfTest {
    public void test() throws Exception {
//        Assert.assertEquals("", CmfTest.init());
    }

    public static $CMF init() throws Exception {
        $ConnConfiguration connConfiguration = new $ConnConfiguration();
        connConfiguration.setIp("127.0.0.1")
                .setPort($.cmf.$PORT)
                .setUsername("root")
                .setPassword("123456")
        ;

        return $.cmf.init(connConfiguration);
    }
}
