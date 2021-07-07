import com.ailbb.ajj.$;
import org.junit.Test;

import static com.ailbb.ajj.$.path;
import static com.ailbb.ajj.$.rel;

public class PathTest {
    public void getTempPath(){
        String confFile = path.getTempPath("kerberos/krb5.conf");

        String p1 = rel("/D:/Z/Code/ailbb/AJT/act/target/classes/kerberos/krb5.conf");

        System.out.println(p1);
        System.out.println(confFile);
        $.file.copyFile(p1, confFile); // 拷贝文件到临时目录（jar包内的，不支持解）
    }
}
