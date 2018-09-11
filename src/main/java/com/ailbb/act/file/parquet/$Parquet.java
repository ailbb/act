package com.ailbb.act.file.parquet;

import com.ailbb.act.$;
import com.ailbb.act.hdfs.$Hdfs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Wz on 9/10/2018.
 */
public class $Parquet {
    private $Hdfs hdfs;

    public $Parquet init($Hdfs hdfs) throws Exception {
        return setHdfs(hdfs);
    }

    /**
     * 读取路径数据
     * @param path
     * @param c
     * @throws IOException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws InstantiationException
     */
    public List<Object> readFiles(String path, Class c) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Group line;
        List<Object> datas = new ArrayList<>();
        ParquetReader<Group> build = ParquetReader.builder(new GroupReadSupport(), new Path(path)).build();

        while ((line = build.read()) != null)
            datas.add(c.getConstructor(Group.class).newInstance(line));

        build.close();

        return datas;
    }

    /**
     * 读取路径数据
     * @param path
     * @throws IOException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws InstantiationException
     */
    public List<Group> readFiles(String path) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Group line;
        List<Group> datas = new ArrayList<>();
        ParquetReader<Group> build = ParquetReader.builder(new GroupReadSupport(), new Path(path)).build();

        while ((line = build.read()) != null)
            datas.add(line);

        build.close();

        return datas;
    }

    /**
     * 读取路径数据
     * @param path
     * @throws IOException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws InstantiationException
     */
    public List<GenericRecord> readGenericRecordFiles(String path) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        GenericRecord line;
        List<GenericRecord> datas = new ArrayList<>();
        ParquetReader build = AvroParquetReader.builder(new Path(path)).build();

        while ((line = (GenericRecord)build.read()) != null)
            datas.add(line);

        build.close();

        return datas;
    }

    /**
     * 读取路径数据
     * @param path
     * @param c
     * @throws IOException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws InstantiationException
     */
    public List<Object> readGenericRecordFiles(String path, Class c) throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        GenericRecord line;
        List<Object> datas = new ArrayList<>();
        ParquetReader build = AvroParquetReader.builder(new Path(path)).build();

        while ((line = (GenericRecord)build.read()) != null)
            datas.add(c.getConstructor(GenericRecord.class).newInstance(line));

        build.close();

        return datas;
    }

    /**
     * 写数据文件
     * @param path
     * @param schema
     * @param o
     * @throws IOException
     */
    public void writeGenericRecordFiles(String path, Schema schema, Object o) throws Exception {
        getParquetSchemaWriter(path, schema).write(o).close();
    }

    /**
     * 写数据文件
     * @param path
     * @param schema
     * @param ol
     * @throws IOException
     */
    public void writeGenericRecordFiles(String path, Schema schema, List<Object> ol) throws Exception {
        getParquetSchemaWriter(path, schema).write(ol).close();
    }

    /**
     * 获取一个Parquet写对象
     * @param path
     * @return
     * @throws IOException
     */
    public $ParquetSchemaWriter getParquetSchemaWriter(String path, Schema schema) throws Exception {
        ParquetWriter parquetWriter;

        try {
            parquetWriter = AvroParquetWriter.builder(getParquetWritePath(path)).withSchema(schema).build();
        } catch (RemoteException e) { // 如果远程创建失败，则重新生成一个文件
            return getParquetSchemaWriter(path, schema);
        }

        return new $ParquetSchemaWriter( parquetWriter, schema );
    }

    /**
     * 获取一个空的路径
     * @param path
     * @return
     * @throws IOException
     */
    public Path getParquetWritePath(String path) throws Exception {
        return hdfs.run(new PrivilegedExceptionAction<Path>() {
            @Override
            public Path run() throws Exception {
                Path outpath = new Path(path);
                FileSystem fs = hdfs.getFileSystem();

                if(fs.exists(outpath) && fs.isFile(outpath)) // 如果路径是已经存在，则删除
                    fs.delete(outpath, false);
                else // 否则，则获取一个随机的文件名
                    return getRandomPath(path);

                return outpath;
            }
        });
    }

    /**
     * 获取一个空的路径
     * @param path
     * @return
     * @throws IOException
     */
    public Path getRandomPath(String path) throws Exception {
        return hdfs.run(new PrivilegedExceptionAction<Path>() {
            @Override
            public Path run() throws Exception {
                Path outpath = new Path(path);
                FileSystem fs = hdfs.getFileSystem();
                int i = 1;

                try {
                    i = fs.listStatus(outpath).length;
                } catch (Exception e) {}

                // 判断路径是否存在
                while(fs.exists(outpath = new Path(path + "/parquet-" + $.string.fill(i++, 6, "0"))));

                return outpath;
            }
        });
    }

    public $Hdfs getHdfs() {
        return hdfs;
    }

    public $Parquet setHdfs($Hdfs hdfs) {
        this.hdfs = hdfs;
        return this;
    }
}
