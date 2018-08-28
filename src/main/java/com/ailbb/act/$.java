package com.ailbb.act;

import com.ailbb.act.cmf.$CMF;
import com.ailbb.act.hbase.$HBase;
import com.ailbb.act.hdfs.$Hdfs;
import com.ailbb.act.hive.$Hive;
import com.ailbb.act.kerberos.$Kerberos;
import org.apache.hadoop.hbase.TableName;

/**
 * Created by Wz on 7/19/2018.
 */
public class $ extends com.ailbb.alt.$ {
    // kerberos
    public static $Kerberos kerberos = new $Kerberos();

    // hive
    public static $Hive hive = new $Hive();

    // hbase
    public static $HBase hbase = new $HBase();

    // hdfs
    public static $Hdfs hdfs = new $Hdfs();

    // cmf
    public static $CMF cmf = new $CMF();

    /**
     * 获取表对象
     * @param tableName
     * @return
     */
    public static TableName toTableName(String tableName) {
        return hbase.toTableName(tableName);
    }
}
