package com.ailbb.act.hbase;

import com.ailbb.act.$;
import com.ailbb.act.exception.$HBaseException;
import com.ailbb.act.hadoop.$Hadoop;
import com.ailbb.act.hdfs.$Hdfs;
import com.ailbb.act.kerberos.$Kerberos;
import com.ailbb.ajj.entity.$Result;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;

/*
 * Hbase操作
 * Created by Wz on 8/10/2018.
 */
public class $HBase extends $Hadoop {
    private Connection connection = null; // 连接
    private Configuration conf; // 配置信息
    private $Hdfs hdfs; // hdfs
    private Admin admin; // admin
    public static final byte[] $FAMILY = "f0".getBytes();

    public $HBase init($Kerberos kerberos, $Hdfs hdfs) throws Exception {
        this.setKerberos(kerberos);

        return init(kerberos.getConf(), hdfs);
    }

    public $HBase init(Configuration conf, $Hdfs hdfs) throws Exception {
        $.info("============== Hbase执行初始化 ==============");

        $HBase hbase = this.setConf(initHbaseConfiguration(conf)).setHdfs(hdfs);

        return this.run(new PrivilegedExceptionAction<$HBase>() {
            @Override
            public $HBase run() throws Exception {
                hbase.setConnection( ConnectionFactory.createConnection(conf) );
                hbase.setAdmin(hbase.getConnection().getAdmin());

                $.info("============== Hbase初始化完成 ==============");
                return hbase;
            }
        });

    }

    /*
     * 初始化Hbase配置
     * @param conf
     * @return
     */
    public Configuration initHbaseConfiguration(Configuration conf){
        if($.isEmptyOrNull(conf)) return HBaseConfiguration.create();

        conf.setClassLoader(HBaseConfiguration.class.getClassLoader());

        try {
//            HeapMemorySizeUtil.checkForClusterFreeMemoryLimit(conf);
            return conf;
        } catch (Exception e) {
            return HBaseConfiguration.addHbaseResources(conf);
        }
    }

    /*
     * 获取hbase的命名空间
     * @return 结果集
     */
    public List<String> getHbaseAllNameSpace() throws Exception {
        return hdfs.getNameSpace("/hbase/data/");
    }

    /*
     * 建表
     * @param tableName 表名称
     * @return 结果集
     */
    public $Result createTable(String tableName)  {
        $Result rs = $.result();

        try {
            this.run(new PrivilegedExceptionAction<$HBase>() {
                @Override
                public $HBase run() throws Exception {
                    TableName tb = toTableName(tableName);
                    if(admin.tableExists(tb))
                        throw new $HBaseException.$TableExistsException("表已经存在：" + tableName);

                    HTableDescriptor td = new HTableDescriptor(tb);
                    td.setDurability(Durability.SKIP_WAL);

                    HColumnDescriptor cd = new HColumnDescriptor($FAMILY);
                    cd.setCompressionType(Compression.Algorithm.SNAPPY);
                    cd.setBlocksize(128 * 1024);
                    cd.setMaxVersions(1);
                    cd.setBloomFilterType(BloomType.NONE);

                    td.addFamily(cd);

                    admin.createTable(td);

                    return $.hbase;
                }
            });
        } catch ($HBaseException.$TableExistsException e) {
            rs.addError($.exception(e)).setSuccess(true); // 可允许出现的错误
        } catch (Exception e) {
            rs.addError($.exception(e));
        }

        return rs;
    }

    /*
     * 删表
     * @param tableName 表名称
     * @return $Result 结构体
     */
    public $Result dropTable(String tableName)  {
        $Result rs = $.result();

        try {
            this.run(new PrivilegedExceptionAction<$HBase>() {
                @Override
                public $HBase run() throws Exception {
                    TableName tb = toTableName(tableName);
                    if(!admin.tableExists(tb))
                        throw new $HBaseException.$TableNotExistsException("表不存在：" + tableName);

                    admin.deleteTable(tb);

                    return $.hbase;
                }
            });
        } catch ($HBaseException.$TableNotExistsException e) {
            rs.addError($.exception(e)).setSuccess(true); // 可允许出现的错误
        } catch (Exception e) {
            rs.addError($.exception(e));
        }

        return rs;
    }

    public $Result close()  {
        $Result rs = $.result();

        try {
            this.run(new PrivilegedExceptionAction<$HBase>() {
                @Override
                public $HBase run() throws Exception {
                    if(null != admin) admin.close();
                    return $.hbase;
                }
            });
        } catch (Exception e) {
            return rs.addError($.exception(e));
        }

        return rs;
    }

    /*
     * 获取表对象
     * @param tableName 表名称
     * @return 表名
     */
    public TableName toTableName(String tableName) {
        return TableName.valueOf(tableName);
    }

    /*
     * GET/SET 方法
     * @return 连接对象
     */

    private Connection getConnection(){
        return connection;
    }

    public $HBase setConnection(Connection connection) {
        this.connection = connection;
        return this;
    }

    public Admin getAdmin() {
        return admin;
    }

    public $HBase setAdmin(Admin admin) {
        this.admin = admin;
        return this;
    }

    public Configuration getConf() {
        return conf;
    }

    public $HBase setConf(Configuration conf) {
        this.conf = conf;
        return this;
    }

    public $Hdfs getHdfs() {
        return hdfs;
    }

    public $HBase setHdfs($Hdfs hdfs) {
        this.hdfs = hdfs;
        return this;
    }
}
