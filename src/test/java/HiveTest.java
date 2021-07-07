import com.ailbb.act.$;
import com.ailbb.act.hive.$Hive;
import com.ailbb.ajj.entity.$JDBCConnConfiguration;
import org.junit.Assert;
import org.junit.Test;

/*
 * Created by Wz on 9/10/2018.
 */
public class HiveTest {
    @Test
    public void test() throws Exception {
//        Assert.assertEquals("", HiveTest.init());
    }

    public static $Hive init() throws Exception {
        $JDBCConnConfiguration jdbcConnConfiguration = new $JDBCConnConfiguration();

        jdbcConnConfiguration
                .setUrl("jdbc:hive2://slave01:10000/default;principal=hive/slave01@CLOUDERA")
                .setDriver($.hive.$DRIVER)
                .setPort($.hive.$PORT)
        ;

        return $.hive.init(jdbcConnConfiguration, KerBerosTest.init());
    }

    public static $Hive init2() throws Exception {
        $JDBCConnConfiguration jdbcConnConfiguration = new $JDBCConnConfiguration();

        jdbcConnConfiguration
                .setDriver($.hive.$DRIVER)
                .setDatabase("default")
                .setIp("slave01")
                .setPort($.hive.$PORT)
        ;

        return $.hive.init(jdbcConnConfiguration);
    }
}
