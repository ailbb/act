import com.ailbb.act.$;
import com.ailbb.act.entity.$KerberosConnConfiguration;
import com.ailbb.act.kerberos.$Kerberos;
import org.apache.hadoop.fs.FileStatus;
import org.junit.Test;
import sun.security.krb5.KrbException;

import java.io.*;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;

/*
 * Created by Wz on 9/10/2018.
 */
public class KerBerosTest {
    public void testKrb5() throws Exception {
        String var1 = "/D:/Program%20Files/Apache/Maven/apache-maven-3.6.3/repository/com/ailbb/act/1.0-SNAPSHOT/act-1.0-SNAPSHOT.jar!/kerberos/krb5.conf";
        ArrayList var2 = new ArrayList();

        BufferedReader var3 = new BufferedReader(new InputStreamReader(new FileInputStream(new File(var1))));
        Throwable var4 = null;

        String var6 = null;

        String var5;
        while ((var5 = var3.readLine()) != null) {
            var5 = var5.trim();
            if (!var5.isEmpty() && !var5.startsWith("#") && !var5.startsWith(";")) {
                if (var5.startsWith("[")) {
                    if (!var5.endsWith("]")) {
                        throw new KrbException("Illegal config content:" + var5);
                    }

                    if (var6 != null) {
                        var2.add(var6);
                        var2.add("}");
                    }

                    String var7 = var5.substring(1, var5.length() - 1).trim();
                    if (var7.isEmpty()) {
                        throw new KrbException("Illegal config content:" + var5);
                    }

                    var6 = var7 + " = {";
                } else if (var5.startsWith("{")) {
                    if (var6 == null) {
                        throw new KrbException("Config file should not start with \"{\"");
                    }

                    var6 = var6 + " {";
                    if (var5.length() > 1) {
                        var2.add(var6);
                        var6 = var5.substring(1).trim();
                    }
                } else if (var6 != null) {
                    var2.add(var6);
                    var6 = var5;
                }
            }
        }

        if (var6 != null) {
            var2.add(var6);
            var2.add("}");
        }


        System.out.println(var2);
    }

    public void t(){
        new $KerberosConnConfiguration().getConfFile();
    }

    public static void doTest() throws Exception {
        java.lang.System.setProperty("java.security.krb5.conf",  "/etc/krb5.conf");
        org.apache.hadoop.conf.Configuration hconf = new org.apache.hadoop.conf.Configuration();
        hconf.set("hadoop.security.authentication", "kerberos");
        hconf.addResource(new org.apache.hadoop.fs.Path("/etc/hadoop/mapred-site.xml"));
        hconf.addResource(new org.apache.hadoop.fs.Path("/etc/hadoopyarn-site.xml"));
        hconf.addResource(new org.apache.hadoop.fs.Path("/etc/hadoop/conf/core-site.xml"));
        hconf.addResource(new org.apache.hadoop.fs.Path("/etc/hadoop/conf/hdfs-site.xml"));
        hconf.addResource(new org.apache.hadoop.fs.Path("/etc/hbase/conf/hbase-site.xml"));
        hconf.addResource(new org.apache.hadoop.fs.Path("/etc/hadoop/conf/yarn-site.xml"));
        hconf.addResource(new org.apache.hadoop.fs.Path("/etc/hive/conf/hive-site.xml"));

        org.apache.hadoop.security.UserGroupInformation.setConfiguration(hconf);
        org.apache.hadoop.security.UserGroupInformation.loginUserFromKeytabAndReturnUGI("hive/db", "/broadtech/share/keytab/hive_db.keytab")
                .doAs(new java.security.PrivilegedExceptionAction() {
                    @Override
                    public Object run() throws Exception {

                            System.out.println("=========Kerberos验证成功，开始打印HDFS目录========== ");

                            FileStatus[] x = org.apache.hadoop.fs.FileSystem.newInstance(hconf).listStatus(new org.apache.hadoop.fs.Path("/"));
                            for(int i=0;i<x.length;i++)
                                System.out.println(x[i].getPath());

                            System.out.println("=========开始执行impala刷新========== ");

                            java.lang.Class.forName("com.cloudera.impala.jdbc41.Driver");
                            java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:impala://192.168.2.138:21050/default;AuthMech=1;KrbHostFQDN=slave31;KrbServiceName=impala", "", "");
                            java.sql.Statement stat = conn.createStatement();

                            try {
                                stat.executeUpdate("refresh prestat_test.st_app_hour ");
                                System.out.println("成功: ");
                            } finally {
                                // db.close();
                                stat.close();
                                conn.close();
                            }


                        return null;
                    }
                });

    }

    static $Kerberos init(){
        return null;
    }
}
