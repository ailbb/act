import com.ailbb.act.$;
import com.ailbb.ajj.entity.$ConnConfiguration;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

public class FtpTest {
    public void t1() throws FileNotFoundException {
        File f = new File("C:\\Users\\demo\\Desktop\\test.log");
        InputStream is = new FileInputStream(f);

        $.ftp.login(new $ConnConfiguration()
                .setIp("127.0.0.1")
                .setPort(21)
                .setUsername("demo_user")
                .setPassword("demo_password")).uploadFile(is, f.length(), "test_dir", "test.log");
    }
}
