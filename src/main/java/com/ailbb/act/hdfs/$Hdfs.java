package com.ailbb.act.hdfs;

import com.ailbb.act.$;
import com.ailbb.act.hadoop.$Hadoop;
import com.ailbb.act.hbase.$HBase;
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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Wz on 8/27/2018.
 */
public class $Hdfs extends $Hadoop {
    private FileSystem fileSystem = null; // hbase
    private Configuration conf; // 配置信息

    public $Hdfs init($Kerberos kerberos) throws Exception {
        this.setKerberos(kerberos);

        return init(kerberos.getConf());
    }

    public $Hdfs init(Configuration conf) throws Exception {
        $.info("============== Hdfs执行初始化 ==============");
        $Hdfs hdfs = this.setConf(conf);

        return this.run(new PrivilegedExceptionAction<$Hdfs>() {
            @Override
            public $Hdfs run() throws Exception {
                hdfs.setFileSystem( FileSystem.get(conf) );

                $.info("============== Hdfs初始化完成 ==============");
                return hdfs;
            }
        });
    }

    /**
     * 获取hbase的命名空间
     * @return 结果集
     */
    public List<String> getNameSpace(String path) throws Exception {
        List<String> list = new ArrayList<>();

        this.run(new PrivilegedExceptionAction<List<String>>() {
            @Override
            public List<String> run() throws Exception {
                FileStatus[] fileStatuses = null;
                fileStatuses = getFileSystem().listStatus(new Path(path));
                for (FileStatus fileStatus : fileStatuses){
                    String[] split = fileStatus.getPath().toString().split("/");
                    list.add(split[split.length-1]);
                }
                return list;
            }
        });

        return list;
    }

    /**
     *
     * 删路径
     * @param path 路径
     * @param recursive 是否递归删除
     * @return $Result 结构体
     */
    public $Result deletePath(String path, boolean recursive)  {
        $Result rs = $.result();

        try {
            return rs.setData(this.run(new PrivilegedExceptionAction<$Hdfs>() {
                @Override
                public $Hdfs run() throws Exception {
                    fileSystem.delete(new Path(path), recursive);
                    return $.hdfs;
                }
            }));
        } catch (Exception e) {
            return rs.addError($.exception(e));
        }
    }

    /**
     * 删路径
     * @return $Result 结构体
     */
    public $Result deletePath(String path)  {
        return deletePath(path, true);
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    public $Hdfs setFileSystem(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
        return this;
    }

    public Configuration getConf() {
        return conf;
    }

    public $Hdfs setConf(Configuration conf) {
        this.conf = conf;
        return this;
    }
}
