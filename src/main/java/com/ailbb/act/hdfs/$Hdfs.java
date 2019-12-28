package com.ailbb.act.hdfs;

import com.ailbb.act.$;
import com.ailbb.act.hadoop.$Hadoop;
import com.ailbb.act.hbase.$HBase;
import com.ailbb.act.kerberos.$Kerberos;
import com.ailbb.ajj.entity.$Result;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.Groups;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static it.unimi.dsi.fastutil.io.TextIO.BUFFER_SIZE;

/**
 * Created by Wz on 8/27/2018.
 */
public class $Hdfs extends $Hadoop {
    private FileSystem fileSystem = null; // hbase
    private Configuration conf; // 配置信息
    private int HDFS_WRITE_BUFSIZE = 512*1024;

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

                $.info("============== 根目录列表 ==============");

                for(String path : hdfs.getNameSpace("/"))  $.info(path);

                return hdfs;
            }
        });
    }

    /**
     * 获取hbase的命名空间
     * @return 结果集
     */
    public List<String> getNameSpace(String path) throws Exception {

        return this.run(new PrivilegedExceptionAction<List<String>>() {
            @Override
            public List<String> run() throws Exception {
                List<String> list = new ArrayList<>();
                FileStatus[] fileStatuses = null;
                fileStatuses = getFileSystem().listStatus(new Path(path));
                for (FileStatus fileStatus : fileStatuses){
                    String[] split = fileStatus.getPath().toString().split("/");
                    list.add(split[split.length-1]);
                }
                return list;
            }
        });
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
            this.run(new PrivilegedExceptionAction<$Hdfs>() {
                @Override
                public $Hdfs run() throws Exception {
                    getFileSystem().delete(new Path(path), recursive);
                    return $.hdfs;
                }
            });
        } catch (Exception e) {
            return rs.addError($.exception(e));
        }

        return rs;
    }

    /**
     * 删路径
     * @return $Result 结构体
     */
    public $Result deletePath(String path)  {
        return deletePath(path, true);
    }

    /**
     * 删路径
     * @return $Result 结构体
     */
    public $Result uploadFile(String path, String descPath)  {
        return uploadFile(path, descPath, true);
    }

    /**
     * 上传文件路径
     * @return $Result 结构体
     */
    public $Result uploadFile(String path, String descPath, boolean overwrite)  {
        $Result rs = $.result();

        try {
            this.run(new PrivilegedExceptionAction<$Hdfs>() {
                @Override
                public $Hdfs run() throws Exception {
                    Path dp = new Path(descPath);
                    $.info("upload " + path + " to " + descPath);

                    try {
                        getFileSystem().copyFromLocalFile(false, overwrite, new Path(path), dp);
                    } catch (Exception e) {
                        $.warn(e);

                        FSDataOutputStream fos;

                        if (getFileSystem().exists(dp) && !overwrite) {
                            fos = getFileSystem().append(dp, HDFS_WRITE_BUFSIZE);
                        } else {
                            fos = getFileSystem().create(dp, overwrite, HDFS_WRITE_BUFSIZE, getFileSystem().getDefaultReplication(dp), getFileSystem().getDefaultBlockSize(dp));
                        }

                        try {
                            IOUtils.copyBytes($.file.getInputStream(path), fos, BUFFER_SIZE, true);
                        } finally {
                            $.file.closeStearm(fos);
                        }
                    }

                    return $.hdfs;
                }
            });
        } catch (Exception e) {
            return rs.addError($.exception(e));
        }

        return rs;
    }

    public FileSystem getFileSystem() throws Exception {
        try {
            if(!$.kerberos.valid()) throw new TimeoutException("验证超时...");  // 验证状态
            fileSystem.getStatus();
        } catch (Exception e) { // hdfs连接被关闭异常
            try {
                init(this.conf); // 重新初始化Hbase连接
            } catch (Exception ex) {
                e.printStackTrace();
                ex.printStackTrace();
                throw ex;
            }
        }

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
        if(null != conf) {
            // 解决java.io.IOException: No FileSystem for scheme: hdfs
            conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.setBoolean("fs.hdfs.impl.disable.cache", true); // 解决hdfs关闭的问题
        }

        this.conf = conf;
        return this;
    }
}
