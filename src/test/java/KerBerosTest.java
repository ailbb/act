import com.ailbb.act.kerberos.$Kerberos;
import org.apache.hadoop.fs.FileStatus;

/**
 * Created by Wz on 9/10/2018.
 */
public class KerBerosTest {
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
