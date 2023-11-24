package com.ailbb.act.file;

import com.ailbb.act.$;
import com.ailbb.ajj.entity.$ConnConfiguration;
import com.ailbb.ajj.entity.$Result;
import com.ailbb.alt.ftp.impl.$XFtp;
import com.ailbb.alt.ftp.impl.$FtpKPI;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/*
 * Created by IvyChoi on 2019/4/2.
 */
public class $FtpCluster extends $XFtp {
    private FileSystem fs;

    public $FtpCluster(String host, int port, String user_name, String password) {
        super(host, port, user_name, password);
        this.logger = $.lastDef($.logger, logger);
        try {
            this.fs = $.lastDef($.hdfs.getFileSystem(), fs);
        } catch (Exception e) {
            logger.warn(e);
        }
    }

    public $FtpCluster($ConnConfiguration connConfiguration) {
        super(connConfiguration);
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

    public $Result uploadDirectory(Path srcPath, String destPath, String prefix, String suffix) {
        $Result res = $.result();
        $FtpKPI kpi = new $FtpKPI();

        try {
            FileStatus[] files = fs.listStatus(srcPath, new PathFilter() {
                @Override
                public boolean accept(Path path) {
                    return !path.getName().startsWith("_") && !path.getName().startsWith(".");
                }
            });
            if (files.length == 0) return res;

            final String destParent = (destPath == null || destPath.isEmpty()) ? "" : !destPath.endsWith("/") ? (destPath + "/") : destPath;
            int index = 0;
            if($.isEmptyOrNull(prefix)) prefix = $.now("ns");
            logger.info("ready to upload " + prefix + " to ftp.");
            long dt = System.currentTimeMillis();

            for (FileStatus f : files) {
                String fileName = String.format(prefix, index) + suffix;
                $Result rs = uploadFile(fs.open(f.getPath()), f.getLen(), destParent, fileName);
                $FtpKPI _kpi = (($FtpKPI)rs.getData());

                kpi.setFilecount(kpi.getFilecount() + _kpi.getFilecount());
                kpi.setRecord(kpi.getRecord() + _kpi.getRecord());
                kpi.setWriteByte(kpi.getWriteByte() + _kpi.getWriteByte());

                if (!rs.isSuccess()) {
                    String msg = "failed to update " + f.getPath().toString() + " with filename " + fileName;
                    logger.error(msg);
                    res.setSuccess(false).addMessage(msg);
                    break;
                }

                index++;
            }

            kpi.setFlow(kpi.getWriteByte() / (System.currentTimeMillis() - dt));
        } catch (Exception e) {
            logger.error(e);
            res.setSuccess(false).addMessage(e.toString());
        }

        return res.setData(kpi);
    }

}
