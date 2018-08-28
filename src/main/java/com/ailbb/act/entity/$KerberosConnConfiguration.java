package com.ailbb.act.entity;

import com.ailbb.ajj.$;
import com.ailbb.ajj.entity.$ConnConfiguration;

/**
 * Created by Wz on 8/21/2018.
 */
public class $KerberosConnConfiguration extends $ConnConfiguration {
    private int rpcTimeOut = 1000 * 200;
    private int operationTimeOut = 1000 * 300;
    private int scannerTimeOut = 1000 * 200;

    private String principal; // hive/db@CLOUDERA
    private String keyTab; // /home/hive/hive_db.keytab
    private String securityAuthentication = "kerberos";
    private String hdfsprefix = "hdfs://myha";
    private String confFile = "/etc/krb5.conf"; // 配置文件
    private String realm; // 非必要
    private String principalUsr; // 非必要

    public int getRpcTimeOut() {
        return rpcTimeOut;
    }

    public $KerberosConnConfiguration setRpcTimeOut(int rpcTimeOut) {
        this.rpcTimeOut = rpcTimeOut;
        return this;
    }

    public int getOperationTimeOut() {
        return operationTimeOut;
    }

    public $KerberosConnConfiguration setOperationTimeOut(int operationTimeOut) {
        this.operationTimeOut = operationTimeOut;
        return this;
    }

    public int getScannerTimeOut() {
        return scannerTimeOut;
    }

    public $KerberosConnConfiguration setScannerTimeOut(int scannerTimeOut) {
        this.scannerTimeOut = scannerTimeOut;
        return this;
    }

    public String getPrincipal() {
        return principal;
    }

    public $KerberosConnConfiguration setPrincipal(String principal) {
        this.principal = principal;
        return this;
    }

    public String getKeyTab() {
        return keyTab;
    }

    public $KerberosConnConfiguration setKeyTab(String keyTab) {
        this.keyTab = keyTab;
        return this;
    }

    public String getSecurityAuthentication() {
        return securityAuthentication;
    }

    public $KerberosConnConfiguration setSecurityAuthentication(String securityAuthentication) {
        this.securityAuthentication = securityAuthentication;
        return this;
    }

    public String getHdfsprefix() {
        return hdfsprefix;
    }

    public $KerberosConnConfiguration setHdfsprefix(String hdfsprefix) {
        this.hdfsprefix = hdfsprefix;
        return this;
    }

    public String getConfFile() {
        return confFile;
    }

    public $KerberosConnConfiguration setConfFile(String confFile) {
        this.confFile = confFile;
        return this;
    }

    public String getPrincipalUsr() {
        try {
            return $.isEmptyOrNull(principalUsr) ? principal.substring(0, principal.indexOf("@")).split("/")[0] : principalUsr;
        } catch (Exception e) {
            return principalUsr;
        }
    }

    public $KerberosConnConfiguration setPrincipalUsr(String principalUsr) {
        this.principalUsr = principalUsr;
        return this;
    }

    public String getRealm() {
        return $.isEmptyOrNull(realm) ? principal.substring(principal.indexOf("@")+1, principal.length()-1).toUpperCase() : realm;
    }

    public $KerberosConnConfiguration setRealm(String realm) {
        this.realm = realm;
        return this;
    }
}
