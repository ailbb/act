import com.ailbb.act.$;
import com.ailbb.act.hbase.$HBase;
import org.junit.Assert;
import org.junit.Test;

/*
 * Created by Wz on 9/10/2018.
 */
public class HBaseTest {
    @Test
    public void test() throws Exception {
//        Assert.assertEquals("", HBaseTest.init());
    }

    public static $HBase init() throws Exception {
        return $.hbase.init(KerBerosTest.init(), HdfsTest.init());
    }

    public static $HBase init2() throws Exception {
        return $.hbase.init(KerBerosTest.init().getConf(), HdfsTest.init());
    }
}
