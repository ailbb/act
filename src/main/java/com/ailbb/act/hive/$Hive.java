package com.ailbb.act.hive;

import com.ailbb.act.$;
import com.ailbb.act.hadoop.$Hadoop;
import com.ailbb.act.kerberos.$Kerberos;
import com.ailbb.ajj.entity.$JDBCConnConfiguration;
import com.ailbb.ajj.entity.$Result;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/*
 * Created by Wz on 8/10/2018.
 */
public class $Hive extends $Hadoop {
    /*
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
    public static final String $DRIVER = "org.apache.hive.jdbc.HiveDriver";
    public static final int $PORT = 10000;

    /*
     * 初始化方法
     * @param connConfiguration
     */
    public $Hive init($JDBCConnConfiguration connConfiguration) throws Exception {
        return init(connConfiguration, null);
    }

    public static void main(String[] args) {

    }
    /*
     * 初始化方法
     * @param connConfiguration
     */
    public $Hive init($JDBCConnConfiguration connConfiguration, $Kerberos kerberos) throws Exception {
        $.info("============== Hive执行初始化 ==============");
        this.setConnConfiguration(connConfiguration);
        this.setKerberos(kerberos);

        DriverManagerDataSource dataSource=new DriverManagerDataSource();
        dataSource.setDriverClassName($.lastDef($DRIVER, connConfiguration.getDriver()));
        dataSource.setUrl(setDataSourceUrl(connConfiguration).getUrl());
        dataSource.setUsername(connConfiguration.getUsername());
        dataSource.setPassword(connConfiguration.getPassword());

        try {
            $.info("获取连接：", connConfiguration.getUrl());

            return setJdbcTemplate(
                    this.run(new PrivilegedExceptionAction<JdbcTemplate>() {
                        @Override
                        public JdbcTemplate run() throws Exception {
                            return new JdbcTemplate(dataSource);
                        }
                    })
            );
        } finally {
            $.info("============== Hive初始化结束 ==============");
        }
    }

    /*
     * 建表
     */
    public $Result getDBS()  {
        return run(String.format("SHOW DATABASES"));
    }

    /*
     * 建表
     */
    public $Result getTables(String db)  {
        return run(String.format("SHOW TABLES from %s", db));
    }

    /*
     * 建表
     */
    public $Result getCols(String db, String table)  {
        return run(String.format("DESC %s.%s", db, table));
    }

    /*
     * 建表
     */
    public $Result createTable(String sql)  {
        return run(sql);
    }

    /*
     * 删表
     */
    public $Result dropTable(String table)  {
        return run(String.format("DROP TABLE IF EXISTS %s", table));
    }

    /*
     * 加载资源表
     */
    public $Result loadTable(String table, String path)  {
        return loadTable(table, path, null);
    }

    /*
     * 加载资源表
     */
    public $Result loadTable(String table, String path, boolean isDelete)  {
        return loadTable(table, path, null, isDelete);
    }

    /*
     * 加载资源表
     */
    public $Result loadTable(String table, String path, Map<String, Object> partition)  {
        return loadTable(table, path, partition, true);
    }

    /*
     * 加载资源表
     */
    public $Result loadTable(String table, String path, Map<String, Object> partition, boolean isDelete)  {
        StringBuffer sql = new StringBuffer();

        sql.append(String.format("load data local inpath '%s' overwrite into table %s", path, table));

        if(!$.isEmptyOrNull(partition)) {
            sql.append("partition(");

            List<String> list = new ArrayList<>();

            for (String key : partition.keySet()){
                list.add(String.format("%s='%s'", key, $.str(partition.get(key))));
            }

            sql.append($.join(list, ","));

            sql.append(")");
        }

        $Result rs = execute(sql.toString());

        if(rs.isSuccess() && isDelete) {
            $.file.delete(path);
        }

        return rs;
    }

    /*
     * 建分区
     * @return
     */
    public $Result createPartition(String table, String path, LinkedHashMap<String, Object> partition)  {
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

    /*
     * 建分区
     * @return
     */
    public $Result createPartition(String table, LinkedHashMap<String, Object> partition)  {
        return createPartition(table, null, partition);
    }

    /*
     * 删分区
     * @return
     */
    public $Result dropPartition(String table, LinkedHashMap<String, Object> partition)  {
        List<Object> params = new ArrayList<>(); // 参数
        List<String> par = new ArrayList<>();

        for(String key : partition.keySet()) {
            params.add(partition.get(key));
            par.add(String.format("%s=?", key));
        }

        return run(String.format("ALTER TABLE ? DROP IF EXISTS PARTITION(%s)", table, $.join(par, ",")), params);
    }

    /*
     * 执行sql
     * @param sql 执行sql
     * @param list 参数
     * @return $Result 结构体
     */
    public $Result run(String sql, List<Object> list)  {
        $Result rs = $.result();

        try {
            rs.setData(this.run(new PrivilegedExceptionAction<Integer>() {
                @Override
                public Integer run() throws Exception {
                    $.info("Run Sql: " + sql);

                    return jdbcTemplate.update(sql,  new PreparedStatementSetter() {
                        public void setValues(PreparedStatement ps) throws SQLException {
                            for(int i=0; i<list.size(); i++) ps.setObject(i+1, list.get(i));
                        }
                    });

                }
            }));
        } catch (Exception e) {
            rs.addError($.exception(e));
        }

        return rs;
    }

    /*
     * 执行sql
     * @param sql 执行sql
     * @return $Result 结构体
     */
    public $Result query(String sql)  {
        $Result rs = $.result();

        try {
            rs.setData(this.run(new PrivilegedExceptionAction<List<Map<String, Object>> >() {
                @Override
                public List<Map<String, Object>>  run() throws Exception {
                    $.info("Run Sql: " + sql);

                    return jdbcTemplate.queryForList(sql);

                }
            }));
        } catch (Exception e) {
            rs.addError($.exception(e));
        }

        return rs;
    }

    /*
     * 执行sql
     * @param sql 执行sql
     * @return $Result 结构体
     */
    public List<Map<String, Object>> queryForList(String sql)  {
        try {
           return this.run(new PrivilegedExceptionAction<List<Map<String, Object>> >() {
                @Override
                public List<Map<String, Object>>  run() throws Exception {
                    $.info("Run Sql: " + sql);

                    return jdbcTemplate.queryForList(sql);

                }
            });
        } catch (Exception e) {
            $.exception(e);
        }

        return new ArrayList<Map<String, Object>>();
    }

    /*
     * 执行sql
     * @param sql 执行sql
     * @return $Result 结构体
     */
    public $Result execute(String sql)  {
        $Result rs = $.result();

        try {
            rs.setData(this.run(new PrivilegedExceptionAction<Boolean>() {
                @Override
                public Boolean run() throws Exception {
                    $.info("Run Sql: " + sql);

                    jdbcTemplate.execute(sql);

                    return true;
                }
            }));
        } catch (Exception e) {
            rs.addError($.exception(e));
        }

        return rs;
    }

    /*
     * 执行sql
     * @param sql 执行sql
     * @return $Result 结构体
     */
    public $Result run(String sql)  {
        return run(sql, new ArrayList<>());
    }

    /*
     * get/set方法
     * @return jdbc连接
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

    /*
     * 获取连接串
     * @return jdbc连接配置
     */
    public $JDBCConnConfiguration setDataSourceUrl($JDBCConnConfiguration connConfiguration) {
        if(!$.isEmptyOrNull(connConfiguration.getUrl())) return connConfiguration;

        List<String> list = new ArrayList<>();
        list.add(String.format("jdbc:hive2://%s:%s/%s", $.notNull(connConfiguration.getIp()), $.lastDef($PORT, connConfiguration.getPort()), $.notNull(connConfiguration.getDatabase())) );

        if(!$.isEmptyOrNull(this.getKerberos()))
            list.add( String.format("principal=%s/%s@%s", $.notNull(this.getKerberos().getKerberosConnConfiguration().getPrincipalUsr()),  $.notNull(connConfiguration.getIp()), this.getKerberos().getKerberosConnConfiguration().getRealm()) );

        return connConfiguration.setUrl($.join(list, ";"));
    }
}
