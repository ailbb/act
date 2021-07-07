import com.ailbb.act.$;
import com.ailbb.act.hdfs.$Hdfs;
import org.junit.Assert;
import org.junit.Test;

/*
 * Created by Wz on 9/10/2018.
 */
public class HdfsTest {
    @Test
    public void test() throws Exception {
//        Assert.assertEquals("", HdfsTest.init());
    }

    public static $Hdfs init() throws Exception {
        return $.hdfs.init(KerBerosTest.init());
    }

    public static $Hdfs init2() throws Exception {
        return $.hdfs.init(KerBerosTest.init().getConf());
    }
}
