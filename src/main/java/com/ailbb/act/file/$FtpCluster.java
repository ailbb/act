package com.ailbb.act.file;

import com.ailbb.act.$;
import com.ailbb.ajj.log.$Logger;
import com.ailbb.alt.ftp.$Ftp;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/*
 * Created by IvyChoi on 2019/4/2.
 */
public class $FtpCluster extends $Ftp {
    private FileSystem fs;

    public $FtpCluster() {}

    public $FtpCluster(String host, int port, String user_name, String password) {
        super(host, port, user_name, password);
        this.logger = $.lastDef($.logger, logger);
        try {
            this.fs = $.lastDef($.hdfs.getFileSystem(), fs);
        } catch (Exception e) {
            logger.warn(e);
        }
    }

    /*
     * 上传文件夹下的所有文件到FTP
     *
     * @param srcPath  源
     * @param destPath 目的路径
     * @return
     * @throws Exception
     */
    public boolean uploadDirectory(Path srcPath, String destPath) {
        boolean res = false;
        try {
            FileStatus[] files = fs.listStatus(srcPath, new PathFilter() {
                @Override
                public boolean accept(Path path) {
                    return !path.getName().startsWith("_") && !path.getName().startsWith(".");
                }
            });
            final String destParent = (destPath == null || destPath.isEmpty()) ? "/" : !destPath.endsWith("/") ? (destPath + "/") : destPath;
            for (FileStatus f : files) {
                res = uploadFile( fs.open(f.getPath()), f.getLen(), destParent, f.getPath().getName()).isSuccess();
                if (!res) break;
            }
        } catch (Exception e) {
            logger.error(e);
        }
        return res;
    }

    /*
     * 上传单个文件
     *
     * @param srcFile
     * @param destPath
     * @return
     */
    public boolean uploadFile(Path srcFile, String destPath) {
        boolean res = false;
        final String destParent = (destPath == null || destPath.isEmpty()) ? "/" : !destPath.endsWith("/") ? (destPath + "/") : destPath;
        try {
            if (fs.exists(srcFile))
                res = uploadFile(fs.open(srcFile), fs.getFileStatus(srcFile).getLen(), destParent, srcFile.getName()).isSuccess();
        } catch (Exception e) {
            logger.error(e);
        }
        return res;
    }

    public boolean uploadDirectory(Path srcPath, String destPath, String prefix, String suffix) {
        boolean res = false;
        try {
            FileStatus[] files = fs.listStatus(srcPath, new PathFilter() {
                @Override
                public boolean accept(Path path) {
                    return !path.getName().startsWith("_") && !path.getName().startsWith(".");
                }
            });
            if (files.length == 0) return true;
            final String destParent = (destPath == null || destPath.isEmpty()) ? "" : !destPath.endsWith("/") ? (destPath + "/") : destPath;
            int index = 0;
            if($.isEmptyOrNull(prefix)) prefix = $.now("ns");
            logger.info("ready to upload " + prefix + " to ftp.");
            for (FileStatus f : files) {
                String fileName = String.format(prefix, index) + suffix;
                res = uploadFile(fs.open(f.getPath()), f.getLen(), destParent, fileName).isSuccess();
                if (!res) {
                    logger.error("failed to update " + f.getPath().toString() + " with filename " + prefix);
                    break;
                }
                index++;
            }
        } catch (Exception e) {
            logger.error(e);
        }
        return res;
    }

}
