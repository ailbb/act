package com.ailbb.act.entity;

import com.ailbb.act.$;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * 集群配置文件
 * Created by Wz on 8/10/2018.
 */
public class $ConfSite {
    private String coreSite;
    private String hdfsSite;
    private String yarnSite;
    private String mapredSite;
    private String hiveSite;
    private String hbaseSite;
    private String sentrySite;
    private String sslClientSite;

    public String getSentrySite() {
        return sentrySite;
    }

    public $ConfSite setSentrySite(String sentrySite) {
        this.sentrySite = sentrySite;
        return this;
    }

    public String getSslClientSite() {
        return sslClientSite;
    }

    public $ConfSite setSslClientSite(String sslClientSite) {
        this.sslClientSite = sslClientSite;
        return this;
    }

    public String getCoreSite() {
        return coreSite;
    }

    public $ConfSite setCoreSite(String coreSite) {
        this.coreSite = coreSite;
        return this;
    }

    public String getHdfsSite() {
        return hdfsSite;
    }

    public $ConfSite setHdfsSite(String hdfsSite) {
        this.hdfsSite = hdfsSite;
        return this;
    }

    public String getYarnSite() {
        return yarnSite;
    }

    public $ConfSite setYarnSite(String yarnSite) {
        this.yarnSite = yarnSite;
        return this;
    }

    public String getMapredSite() {
        return mapredSite;
    }

    public $ConfSite setMapredSite(String mapredSite) {
        this.mapredSite = mapredSite;
        return this;
    }

    public String getHiveSite() {
        return hiveSite;
    }

    public $ConfSite setHiveSite(String hiveSite) {
        this.hiveSite = hiveSite;
        return this;
    }

    public String getHbaseSite() {
        return hbaseSite;
    }

    public $ConfSite setHbaseSite(String hbaseSite) {
        this.hbaseSite = hbaseSite;
        return this;
    }

    public Configuration getConfiguration() {
        return addResource(new Configuration());
    }

    public Configuration addResource(Configuration conf) {
        tryAddResource(conf, getCoreSite(), "core-site");
        tryAddResource(conf, getHiveSite(), "hive-site");
        tryAddResource(conf, getHdfsSite(), "hdfs-site");
        tryAddResource(conf, getHbaseSite(), "hbase-site");
        tryAddResource(conf, getSentrySite(), "sentry-site");
        tryAddResource(conf, getYarnSite(), "yarn-site");
        tryAddResource(conf, getMapredSite(), "mapred-site");
        tryAddResource(conf, getSslClientSite(), "ssl-client-site");
        return conf;
    }

    /**
     * 防止添加内容出错
     * @param conf
     * @param path
     * @param name
     * @return
     */
    public Configuration tryAddResource(Configuration conf, String path, String name) {
        try {
            if(!$.isEmptyOrNull(path)) {
                conf.addResource(new Path(path));
            } else {
                $.warn(name + " is null or empty.");
            }
        } catch (Exception e) {
            $.warn(name + " has not found.");
        }

        return conf;
    }
}
