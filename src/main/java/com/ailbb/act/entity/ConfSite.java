package com.ailbb.act.entity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * 集群配置文件
 * Created by Wz on 8/10/2018.
 */
public class ConfSite {
    private String coreSite;
    private String hdfsSite;
    private String yarnSite;
    private String mapredSite;
    private String hiveSite;
    private String hbaseSite;

    public String getCoreSite() {
        return coreSite;
    }

    public ConfSite setCoreSite(String coreSite) {
        this.coreSite = coreSite;
        return this;
    }

    public String getHdfsSite() {
        return hdfsSite;
    }

    public ConfSite setHdfsSite(String hdfsSite) {
        this.hdfsSite = hdfsSite;
        return this;
    }

    public String getYarnSite() {
        return yarnSite;
    }

    public ConfSite setYarnSite(String yarnSite) {
        this.yarnSite = yarnSite;
        return this;
    }

    public String getMapredSite() {
        return mapredSite;
    }

    public ConfSite setMapredSite(String mapredSite) {
        this.mapredSite = mapredSite;
        return this;
    }

    public String getHiveSite() {
        return hiveSite;
    }

    public ConfSite setHiveSite(String hiveSite) {
        this.hiveSite = hiveSite;
        return this;
    }

    public String getHbaseSite() {
        return hbaseSite;
    }

    public ConfSite setHbaseSite(String hbaseSite) {
        this.hbaseSite = hbaseSite;
        return this;
    }

    public Configuration getConfiguration() {
        return addResource(new Configuration());
    }

    public Configuration addResource(Configuration conf) {
        conf.addResource(new Path(getCoreSite()));
        conf.addResource(new Path(getHbaseSite()));
        conf.addResource(new Path(getYarnSite()));
        conf.addResource(new Path(getMapredSite()));
        conf.addResource(new Path(getHiveSite()));
        conf.addResource(new Path(getHbaseSite()));
        return conf;
    }
}
