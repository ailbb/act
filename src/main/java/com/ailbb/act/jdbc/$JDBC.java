package com.ailbb.act.jdbc;

import com.ailbb.act.$;
import com.ailbb.act.hive.$Hive;
import com.ailbb.act.kerberos.$Kerberos;
import com.ailbb.ajj.entity.$JDBCConnConfiguration;

import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;

public class $JDBC extends com.ailbb.ajj.jdbc.$JDBC {

    /*
     * 初始化方法
     * @param connConfiguration
     */
    public Connection getConnection($JDBCConnConfiguration connConfiguration, $Kerberos kerberos) throws Exception {
        $.info("============== 获取Hive连接 ==============");

        try {
            $.info("获取连接：", connConfiguration.getUrl());
            Class.forName($.lastDef($Hive.$DRIVER, connConfiguration.getDriver()));

            PrivilegedExceptionAction<Connection> pa = new PrivilegedExceptionAction<Connection>() {
                @Override
                public Connection run() throws Exception {
                    return DriverManager.getConnection(connConfiguration.getUrl(), connConfiguration.getUsername(), connConfiguration.getPassword());
                }
            };

            return null != kerberos && kerberos.isEnable() ? kerberos.getUgi().doAs(pa) : pa.run();
        } catch (Exception e){
            $.warn("获取连接失败：", connConfiguration);
            $.error(e);
        }

        return null;
    }

}
