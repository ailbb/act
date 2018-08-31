package com.ailbb.act.kerberos;

import com.ailbb.act.entity.$KerberosConnConfiguration;
import com.ailbb.act.entity.$ConfSite;
import com.ailbb.ajj.$;
import com.ailbb.ajj.entity.$Result;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 * Kerberos验证
 * Created by Wz on 2017/12/5.
 */
public class $Kerberos {
    private UserGroupInformation ugi; // 用户信息
    private Configuration conf; // 配置信息
    private $ConfSite confSite; // 配置信息
    private $KerberosConnConfiguration kerberosConnConfiguration;

    /**
     * 执行初始化
     * @param kerberosConnConfiguration kerberos连接配置对象
     * @param confSite xml路径配置
     * @return 当前对象
     */
    public $Kerberos init($KerberosConnConfiguration kerberosConnConfiguration, $ConfSite confSite){
        $.info("============== kerberos执行初始化 ==============");
        this.setKerberosConnConfiguration(kerberosConnConfiguration).setConfSite(confSite).valid(); // 设置配置
        return this;
    }

    /**
     * 验证方法
     * @return 是否成功
     */
    public boolean valid() {
        try {
            $.info("============== kerberos开始验证 ==============");

            if(!doCheck()) {
                doConfig(); // 填写配置信息
                outInfo(); // 打印输出信息
                if(doLogin().isSuccess()) { // 进行登录认证
                    $.info("============== kerberos验证成功 ==============");
                } else {
                    $.error("============== kerberos验证失败 ==============");
                }
            }

            return true;
        } catch (Exception e) {
            $.exception(e);
            return false;
        }
    }

    /**
     * @return 连接配置
     */
    public Configuration doConfig() {
        conf = confSite.addResource(new Configuration()); // 添加资源文件

        // 设置超时时间
        conf.setInt("hbase.rpc.timeout", kerberosConnConfiguration.getRpcTimeOut());
        conf.setInt("hbase.client.operation.timeout", kerberosConnConfiguration.getOperationTimeOut());
        conf.setInt("hbase.client.scanner.timeout.period", kerberosConnConfiguration.getScannerTimeOut());

        // 解决java.io.IOException: No FileSystem for scheme: hdfs
        conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

        conf.setBoolean("hadoop.security.authorization", true);

        System.setProperty("java.security.krb5.conf", kerberosConnConfiguration.getConfFile());

        // 设置keyberos信息
        conf.set("PRINCIPAL", kerberosConnConfiguration.getPrincipal());
        conf.set("KEYTAB", kerberosConnConfiguration.getKeyTab());
        conf.set("hadoop.security.authentication", kerberosConnConfiguration.getSecurityAuthentication());

        return conf;
    }

    /**
     * @return $Result 结构体
     */
    public $Result doLogin()  {
        $Result rs = $.result();
        try {
            UserGroupInformation.setConfiguration(conf);
            this.ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(conf.get("PRINCIPAL"), conf.get("KEYTAB"));
            rs.addMessage($.error("============== 登录成功 =============="));
        } catch (IOException e) {
            rs.addError($.exception(e)).addMessage($.error("============== 登录失败 =============="));
        }

        return rs;
    }

    /**
     * @return 是否成功
     */
    public boolean doCheck() {
        try {
            ugi.checkTGTAndReloginFromKeytab();
        } catch (Exception e) {
            return false;
        }

        return true;
    }

    public <T> T run(PrivilegedExceptionAction<T> action) throws Exception {
        return doAs(action);
    }

    public <T> T doAs(PrivilegedExceptionAction<T> action) throws Exception {
        return $.isEmptyOrNull(this.getUgi()) ? action.run() : this.getUgi().doAs(action);
    }

    /**
     * 打印验证消息
     */
    private void outInfo(){
        $.info("============== kerberos验证信息 ==============");
        
        $.info(String.format("coreSite = %s", confSite.getCoreSite()));
        $.info(String.format("hdfsSite = %s", confSite.getHdfsSite()));
        $.info(String.format("yarnSite = %s", confSite.getYarnSite()));
        $.info(String.format("mapredSite = %s", confSite.getMapredSite()));
        $.info(String.format("hiveSite = %s", confSite.getHiveSite()));
        $.info(String.format("hbaseSite = %s", confSite.getHbaseSite()));
        
        $.info(String.format("hbase.rpc.timeout = %s", kerberosConnConfiguration.getRpcTimeOut()));
        $.info(String.format("hbase.client.operation.timeout = %s", kerberosConnConfiguration.getOperationTimeOut()));
        $.info(String.format("hbase.client.scanner.timeout.period = %s", kerberosConnConfiguration.getScannerTimeOut()));
        
        $.info(String.format("PRINCIPAL = %s", kerberosConnConfiguration.getPrincipal()));
        $.info(String.format("KEYTAB = %s", kerberosConnConfiguration.getKeyTab()));
        $.info(String.format("hadoop.security.authentication = %s", kerberosConnConfiguration.getSecurityAuthentication()));
        
        $.info(String.format("用户信息 = %s", ugi.getShortUserName()));
        $.info(String.format("是否需要验证 = %s", UserGroupInformation.isSecurityEnabled()));
    }

    public UserGroupInformation getUgi() {
        return ugi;
    }

    public $Kerberos setUgi(UserGroupInformation ugi) {
        this.ugi = ugi;
        return this;
    }

    public Configuration getConf() {
        return conf;
    }

    public $ConfSite getConfSite() {
        return confSite;
    }

    public $Kerberos setConfSite($ConfSite confSite) {
        this.confSite = confSite;
        return this;
    }

    public $KerberosConnConfiguration getKerberosConnConfiguration() {
        return kerberosConnConfiguration;
    }

    public $Kerberos setKerberosConnConfiguration($KerberosConnConfiguration kerberosConnConfiguration) {
        this.kerberosConnConfiguration = kerberosConnConfiguration;
        return this;
    }
}
