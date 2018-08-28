package com.ailbb.act.hive;

import com.ailbb.act.$;
import com.ailbb.act.hadoop.$Hadoop;
import com.ailbb.act.kerberos.$Kerberos;
import com.ailbb.ajj.entity.$JDBCConnConfiguration;
import com.ailbb.ajj.entity.$Result;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Wz on 8/10/2018.
 */
public class $Hive extends $Hadoop {
    /**
     * demo
     *
     private String driver = "org.apache.hive.jdbc.HiveDriver";
     private String ip = "master01";
     private int port = 10000;
     private String database = "default";
     private String username;
     private String password;
     private String url = "jdbc:hive2://master01:10000/default;principal=hive/master01@CLOUDERA";
     *
     */
    private $JDBCConnConfiguration connConfiguration; // 连接配置信息
    private JdbcTemplate jdbcTemplate;
    public final String $DRIVER = "org.apache.hive.jdbc.HiveDriver";
    public final int $PORT = 10000;

    /**
     * 初始化方法
     * @param connConfiguration
     * @return
     */
    public $Hive init($JDBCConnConfiguration connConfiguration) {
        return init(connConfiguration, null);
    }

    /**
     * 初始化方法
     * @param connConfiguration
     * @return
     */
    public $Hive init($JDBCConnConfiguration connConfiguration, $Kerberos kerberos) {
        $.info("============== Hive执行初始化 ==============");
        this.setConnConfiguration(connConfiguration);
        this.setKerberos(kerberos);

        DriverManagerDataSource dataSource=new DriverManagerDataSource();
        dataSource.setDriverClassName($.lastDef($DRIVER, connConfiguration.getDriver()));
        dataSource.setUrl(getDataSourceUrl());
        dataSource.setUsername(connConfiguration.getUsername());
        dataSource.setPassword(connConfiguration.getPassword());

        try {
            return setJdbcTemplate(
                    this.run(new PrivilegedExceptionAction<JdbcTemplate>() {
                        @Override
                        public JdbcTemplate run() throws Exception {
                            return new JdbcTemplate(dataSource);
                        }
                    })
            );
        } catch (IOException e) {
            $.exception(e);
        } catch (InterruptedException e) {
            $.exception(e);
        } catch (Exception e) {
            $.exception(e);
        }

        return this;
    }

    /**
     * 建表
     * @return
     */
    public $Result createTable(String sql) {
        return run(sql);
    }

    /**
     * 删表
     * @return
     */
    public $Result dropTable(String table) {
        return run(String.format("DROP TABLE IF EXISTS %s", table));
    }

    /**
     * 建分区
     * @return
     */
    public $Result createPartition(String table, String path, LinkedHashMap<String, String> partition) {
        List<Object> params = new ArrayList<>(); // 参数
        List<String> par = new ArrayList<>();

        for(String key : partition.keySet()) {
            params.add(partition.get(key));
            par.add(String.format("%s=?", key));
        }

        String sql = String.format("ALTER TABLE %s ADD IF NOT EXISTS PARTITION (%s)", table, $.join(par, ","));

        if(!$.isEmptyOrNull(path)) sql += String.format(" location '%s'", path);

        return run(sql, params);
    }

    /**
     * 建分区
     * @return
     */
    public $Result createPartition(String table, LinkedHashMap<String, String> partition) {
        return createPartition(table, null, partition);
    }

    /**
     * 删分区
     * @return
     */
    public $Result dropPartition(String table, LinkedHashMap<String, String> partition) {
        List<Object> params = new ArrayList<>(); // 参数
        List<String> par = new ArrayList<>();

        for(String key : partition.keySet()) {
            params.add(partition.get(key));
            par.add(String.format("%s=?", key));
        }

        return run(String.format("ALTER TABLE ? DROP IF EXISTS PARTITION(%s)", table, $.join(par, ",")), params);
    }

    /**
     * 执行sql
     * @param sql
     * @return
     */
    public $Result run(String sql, List<Object> list) {
        try {
            this.run(new PrivilegedExceptionAction<$Hive>() {
                @Override
                public $Hive run() throws Exception {
                    jdbcTemplate.update(sql,  new PreparedStatementSetter() {
                        public void setValues(PreparedStatement ps) throws SQLException {
                            for(int i=0; i<list.size(); i++) ps.setObject(i+1, list.get(i));
                        }
                    });

                    return $.hive;
                }
            });
        } catch (Exception e) {
            $.exception(e);
        }

        return $.result();
    }

    /**
     * 执行sql
     * @param sql
     * @return
     */
    public $Result run(String sql) {
        return run(sql, new ArrayList<>());
    }

    /**
     * get/set方法
     * @return
     */
    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    public $Hive setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        return this;
    }

    public $JDBCConnConfiguration getConnConfiguration() {
        return connConfiguration;
    }

    public $Hive setConnConfiguration($JDBCConnConfiguration connConfiguration) {
        this.connConfiguration = connConfiguration;
        return this;
    }

    /**
     * 获取连接串
     * @return
     */
    public String getDataSourceUrl() {
        if(!$.isEmptyOrNull(connConfiguration.getUrl())) return connConfiguration.getUrl();

        List<String> list = new ArrayList<>();
        list.add(String.format("jdbc:hive2://%s:%s/%s", $.notNull(connConfiguration.getIp()), $.lastDef($PORT, connConfiguration.getPort()), $.notNull(connConfiguration.getDatabase())) );

        if(!$.isEmptyOrNull(this.getKerberos()))
            list.add( String.format("principal=%s/%s@%s", $.notNull(this.getKerberos().getKerberosConnConfiguration().getPrincipalUsr()),  $.notNull(connConfiguration.getIp()), this.getKerberos().getKerberosConnConfiguration().getRealm()) );

        return $.join(list, ";");
    }
}
