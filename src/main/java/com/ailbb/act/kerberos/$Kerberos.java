package com.ailbb.act.kerberos;

import com.ailbb.act.entity.ConfSite;
import com.ailbb.act.exception.$KerberosException;
import com.ailbb.ajj.$;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * Kerberos验证
 * Created by Wz on 2017/12/5.
 */
public class $Kerberos {
    private UserGroupInformation ugi; // 用户信息
    private Configuration conf; // 配置信息
    private ConfSite confSite; // 配置信息

    private int rpcTimeOut = 200000;
    private int operationTimeOut = 300000;
    private int scannerTimeOut = 200000;
    
    private String principal; // "brd"
    private String keyTab; // /home/brd/brd.keytab
    private String securityAuthentication = "kerberos";
    
    /**
     * 验证方法
     * @return
     */
    public boolean valid() throws $KerberosException.NullPointKeyTabException, $KerberosException.NullPointPrincipalException, $KerberosException.NullPointConfSizeException {
        try {
            $.info("============== kerberos开始验证 ==============");

            if(!doCheck()) {
                doConfig(); // 填写配置信息
                doLogin(); // 进行登录认证
                outInfo(); // 打印输出信息
            }

            $.info("============== kerberos验证成功 ==============");
            return true;
        } catch ($KerberosException.NullPointConfSizeException | $KerberosException.NullPointPrincipalException | $KerberosException.NullPointKeyTabException e) {
            $.error("============== " +e.getMessage()+ " ==============");
            throw e;
        } catch (IOException e) {
            $.error("============== 登录失败 ==============");
            $.exception(e);
            return false;
        } catch (Exception e) {
            $.error("============== kerberos验证失败 ==============");
            $.exception(e);
            return false;
        }
    }

    /**
     *
     * @return
     */
    private Configuration doConfig() throws $KerberosException.NullPointConfSizeException, $KerberosException.NullPointKeyTabException, $KerberosException.NullPointPrincipalException {

        if($.isEmptyOrNull(confSite)) throw new $KerberosException.NullPointConfSizeException("ConfSite对象为空");

        if($.isEmptyOrNull(principal)) throw new $KerberosException.NullPointPrincipalException("Principal对象为空");

        if($.isEmptyOrNull(keyTab)) throw new $KerberosException.NullPointKeyTabException("KeyTab对象为空");

        conf = confSite.addResource(new Configuration()); // 添加资源文件

        // 设置超时时间
        conf.setInt("hbase.rpc.timeout", rpcTimeOut);
        conf.setInt("hbase.client.operation.timeout", operationTimeOut);
        conf.setInt("hbase.client.scanner.timeout.period", scannerTimeOut);

        // 设置keyberos信息
        conf.set("PRINCIPAL", principal);
        conf.set("KEYTAB", keyTab);
        conf.set("hadoop.security.authentication", securityAuthentication);

        return conf;
    }

    /**
     *
     * @return
     */
    private UserGroupInformation doLogin() throws IOException {
        UserGroupInformation.setConfiguration(conf);

        ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(conf.get("PRINCIPAL"), conf.get("KEYTAB"));

        return ugi;
    }

    /**
     *
     * @return
     */
    private boolean doCheck() {
        try {
            ugi.checkTGTAndReloginFromKeytab();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     *
     */
    private void outInfo(){
        $.info("============== kerberos验证信息 ==============");
        
        $.info(String.format("coreSite = %s", confSite.getCoreSite()));
        $.info(String.format("hdfsSite = %s", confSite.getHdfsSite()));
        $.info(String.format("yarnSite = %s", confSite.getYarnSite()));
        $.info(String.format("mapredSite = %s", confSite.getMapredSite()));
        $.info(String.format("hiveSite = %s", confSite.getHiveSite()));
        $.info(String.format("hbaseSite = %s", confSite.getHbaseSite()));
        
        $.info(String.format("hbase.rpc.timeout = %s", rpcTimeOut));
        $.info(String.format("hbase.client.operation.timeout = %s", operationTimeOut));
        $.info(String.format("hbase.client.scanner.timeout.period = %s", scannerTimeOut));
        
        $.info(String.format("PRINCIPAL = %s", principal));
        $.info(String.format("KEYTAB = %s", keyTab));
        $.info(String.format("hadoop.security.authentication = %s", securityAuthentication));
        
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

    public $Kerberos setConf(Configuration conf) {
        this.conf = conf;
        return this;
    }

    public ConfSite getConfSite() {
        return confSite;
    }

    public $Kerberos setConfSite(ConfSite confSite) {
        this.confSite = confSite;
        return this;
    }

    public int getRpcTimeOut() {
        return rpcTimeOut;
    }

    public $Kerberos setRpcTimeOut(int rpcTimeOut) {
        this.rpcTimeOut = rpcTimeOut;
        return this;
    }

    public int getOperationTimeOut() {
        return operationTimeOut;
    }

    public $Kerberos setOperationTimeOut(int operationTimeOut) {
        this.operationTimeOut = operationTimeOut;
        return this;
    }

    public int getScannerTimeOut() {
        return scannerTimeOut;
    }

    public $Kerberos setScannerTimeOut(int scannerTimeOut) {
        this.scannerTimeOut = scannerTimeOut;
        return this;
    }

    public String getPrincipal() {
        return principal;
    }

    public $Kerberos setPrincipal(String principal) {
        this.principal = principal;
        return this;
    }

    public String getKeyTab() {
        return keyTab;
    }

    public $Kerberos setKeyTab(String keyTab) {
        this.keyTab = keyTab;
        return this;
    }

    public String getSecurityAuthentication() {
        return securityAuthentication;
    }

    public $Kerberos setSecurityAuthentication(String securityAuthentication) {
        this.securityAuthentication = securityAuthentication;
        return this;
    }
}
