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

    public $Hdfs init($Kerberos kerberos){
        this.setKerberos(kerberos);

        return init(kerberos.getConf());
    }

    public $Hdfs init(Configuration conf){
        $.info("============== Hdfs执行初始化 ==============");
        $Hdfs hdfs = this.setConf(conf);

        try {
            return this.run(new PrivilegedExceptionAction<$Hdfs>() {
                @Override
                public $Hdfs run() throws Exception {
                    hdfs.setFileSystem( FileSystem.get(conf) );

                    return hdfs;
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
    public $Result getNameSpace(String path){
        List<String> list = new ArrayList<>();

        try {
            this.run(new PrivilegedExceptionAction<List<String>>() {
                @Override
                public List<String> run() throws Exception {
                    FileStatus[] fileStatuses = null;
                    try {
                        fileStatuses = getFileSystem().listStatus(new Path(path));
                        for (FileStatus fileStatus : fileStatuses){
                            String[] split = fileStatus.getPath().toString().split("/");
                            list.add(split[split.length-1]);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    return list;
                }
            });
        } catch (Exception e) {
            $.exception(e);
        }

        return $.result().setData(list);
    }

    /**
     *
     * 删路径
     * @param path
     * @param recursive 是否递归删除
     * @return
     */
    public $Result deletePath(String path, boolean recursive) {
        $Hdfs hdfs = this;

        try {
            this.run(new PrivilegedExceptionAction<$Hdfs>() {
                @Override
                public $Hdfs run() throws Exception {
                    try {
                        fileSystem.delete(new Path(path), recursive);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    return hdfs;
                }
            });
        } catch (Exception e) {
            $.exception(e);
        }

        return $.result().setData(hdfs);
    }

    /**
     * 删路径
     * @return
     */
    public $Result deletePath(String path) {
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
