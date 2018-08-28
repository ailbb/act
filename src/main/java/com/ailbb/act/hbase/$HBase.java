package com.ailbb.act.hbase;

import com.ailbb.act.$;
import com.ailbb.act.hadoop.$Hadoop;
import com.ailbb.act.hdfs.$Hdfs;
import com.ailbb.act.kerberos.$Kerberos;
import com.ailbb.ajj.entity.$Result;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

/**
 * Hbase操作
 * Created by Wz on 8/10/2018.
 */
public class $HBase extends $Hadoop {
    private Connection connection = null; // 连接
    private Configuration conf; // 配置信息
    private $Hdfs hdfs; // hdfs
    private Admin admin; // admin
    public final byte[] $FAMILY = "f0".getBytes();

    public $HBase init($Kerberos kerberos, $Hdfs hdfs){
        this.setKerberos(kerberos);

        return init(kerberos.getConf(), hdfs);
    }

    public $HBase init(Configuration conf, $Hdfs hdfs){
        $.info("============== Hbase执行初始化 ==============");
        $HBase hbase = this.setConf(conf);
        this.setHdfs(hdfs);

        try {
            return this.run(new PrivilegedExceptionAction<$HBase>() {
                @Override
                public $HBase run() throws Exception {
                    hbase.setConnection( ConnectionFactory.createConnection(conf) );
                    hbase.setAdmin(hbase.getConnection().getAdmin());

                    return hbase;
                }
            });
        } catch (Exception e) {
            $.exception(e);
        }

        return this;
    }

    /**
     * 获取hbase的命名空间
     * @return
     */
    public $Result getHbaseAllNameSpace(){
        return hdfs.getNameSpace("/hbase/data/");
    }

    /**
     * 建表
     * @return
     */
    public $Result createTable(String tableName) {
        $HBase hbase = this;

        try {
            this.run(new PrivilegedExceptionAction<$HBase>() {
                @Override
                public $HBase run() throws Exception {
                    TableName tb = toTableName(tableName);
                    try {
                        if(admin.tableExists(tb)) return hbase;

                        HTableDescriptor td = new HTableDescriptor(tb);
                        td.setDurability(Durability.SKIP_WAL);

                        HColumnDescriptor cd = new HColumnDescriptor($FAMILY);
                        cd.setCompressionType(Compression.Algorithm.SNAPPY);
                        cd.setBlocksize(128 * 1024);
                        cd.setMaxVersions(1);
                        cd.setBloomFilterType(BloomType.NONE);

                        td.addFamily(cd);

                        admin.createTable(td);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    return hbase;
                }
            });
        } catch (Exception e) {
            $.exception(e);
        }

        return $.result().setData(hbase);
    }

    /**
     * 删表
     * @return
     */
    public $Result dropTable(String tableName) {
        $HBase hbase = this;

        try {
            this.run(new PrivilegedExceptionAction<$HBase>() {
                @Override
                public $HBase run() throws Exception {
                    TableName tb = toTableName(tableName);
                    try {
                        if(!admin.tableExists(tb)) return hbase;

                        admin.deleteTable(tb);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    return hbase;
                }
            });
        } catch (Exception e) {
            $.exception(e);
        }

        return $.result().setData(hbase);
    }

    public $HBase close(){
        $HBase hbase = this;

        try {
            return this.run(new PrivilegedExceptionAction<$HBase>() {
                @Override
                public $HBase run() throws Exception {
                    try {
                        if(null != admin) admin.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    return hbase;
                }
            });
        } catch (Exception e) {
            $.exception(e);
        }

        return this;
    }

    /**
     * 获取表对象
     * @param tableName
     * @return
     */
    public TableName toTableName(String tableName) {
        return TableName.valueOf(tableName);
    }

    /**
     * GET/SET 方法
     * @return
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
