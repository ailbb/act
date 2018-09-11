package com.ailbb.act;

import com.ailbb.act.cmf.$CMF;
import com.ailbb.act.file.$File;
import com.ailbb.act.hbase.$HBase;
import com.ailbb.act.hdfs.$Hdfs;
import com.ailbb.act.hive.$Hive;
import com.ailbb.act.kafka.$Kafka;
import com.ailbb.act.kerberos.$Kerberos;
import com.ailbb.act.mapreduce.$MapReduce;
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

    // kafka
    public static $Kafka kafka = new $Kafka();

    // cmf
    public static $CMF cmf = new $CMF();

    // cmf
    public static $MapReduce mapReduce = new $MapReduce();

    // file
    public static $File file = new $File();

    /**
     * 获取表对象
     * @param tableName
     * @return TableName type
     */
    public static TableName toTableName(String tableName) {
        return hbase.toTableName(tableName);
    }
}
