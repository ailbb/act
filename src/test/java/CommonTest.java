import com.ailbb.act.$;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by Wz on 9/10/2018.
 */
public class CommonTest {
    @Test
    public void test() throws Exception {
        for(int i=10; i-->0;)
            Assert.assertEquals("00000" + i, $.string.fill(i, 6, "0"));
    }
}
