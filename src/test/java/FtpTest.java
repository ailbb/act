import com.ailbb.act.$;
import com.ailbb.ajj.entity.$ConnConfiguration;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

public class FtpTest {
    public void t1() throws FileNotFoundException {
        File f = new File("C:\\Users\\sirzh\\Desktop\\courgette.log");
        InputStream is = new FileInputStream(f);

        $.ftp.login(new $ConnConfiguration()
                .setIp("192.168.5.129")
                .setPort(21)
                .setUsername("brd")
                .setPassword("zaq1.;[=")).uploadFile(is, f.length(), "test7", "abc.log");
    }
}
