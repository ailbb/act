package com.ailbb.act.mapreduce;

import com.ailbb.act.$;
import com.ailbb.act.entity.$MapReduceConfiguration;
import com.ailbb.act.hdfs.$Hdfs;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.avro.AvroParquetInputFormat;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 * Created by Wz on 9/10/2018.
 */
public class $MapReduce extends Configured implements Tool {
    private $Hdfs hdfs;
    private Job job;
    private $MapReduceConfiguration mapReduceConfiguration;

    public $MapReduce init($Hdfs hdfs, $MapReduceConfiguration mapReduceConfiguration) throws Exception {
        return setMapReduceConfiguration(mapReduceConfiguration).setHdfs(hdfs).getHdfs().run(new PrivilegedExceptionAction<$MapReduce>(){
            @Override
            public $MapReduce run() throws Exception {
                return initJob();
            }
        });
    }

    /**
     * 使用ToolRunner执行run
     * @param args
     * @throws Exception
     */
    public void toolRun(String[] args) throws Exception {
        ToolRunner.run(this, args);
    }

    @Override
    public int run(String[] args) throws Exception {
        return runJob() ? 0 : 1;
    }

    public boolean runJob() throws InterruptedException, IOException, ClassNotFoundException {
        $.sout("============== 执行任务：" + mapReduceConfiguration.getJobName() + " ==============");
        return job.waitForCompletion(true);
    }

    /**
     * 初始化Job
     */
    private $MapReduce initJob() throws Exception {
        // 添加路径下面的所有jar
        Job job = Job.getInstance(hdfs.getConf(), mapReduceConfiguration.getJobName());
        FileSystem fs = hdfs.getFileSystem();

        // 添加路径下面的所有jar
        for(String p : mapReduceConfiguration.getJarPath()) {
            for( FileStatus f : fs.listStatus(new Path(p)) )
                job.addFileToClassPath(f.getPath());
        }
        job.setJarByClass($.isEmptyOrNull(mapReduceConfiguration.getJarClass()) ? this.getClass() : mapReduceConfiguration.getJarClass());

        // 1.7:指定map输出的<K,V>类型
        job.setMapOutputKeyClass(mapReduceConfiguration.getMapOutputKeyClass());
        job.setMapOutputValueClass(mapReduceConfiguration.getMapOutputValueClass());

        // 1.7:指定reduce输出的<K,V>类型
        job.setOutputKeyClass(mapReduceConfiguration.getReduceOutputKeyClass());
        job.setOutputValueClass(mapReduceConfiguration.getReduceOutputValueClass());

        // 1.2:指定自定义的Mapper类
        job.setMapperClass(mapReduceConfiguration.getMapper());
        // 1.6:指定自定义的Reducer类
        job.setReducerClass(mapReduceConfiguration.getReducer());

        // 1.1:指定对输入数据进行格式化处理的类（可以省略）
        job.setInputFormatClass(mapReduceConfiguration.getInputFormatClass());
        // 1.9:指定对输出数据进行格式化处理的类
        job.setOutputFormatClass(mapReduceConfiguration.getOutputFormatClass());
//        AvroParquetOutputFormat.setSchema(job, SleepTimesConfig.getSchema());

//        MessageType msc = new AvroSchemaConverter().convert(SchemaUtils.getDefaultSchema(this.getJarClass()));

        // 1.5:设置要运行的Reducer的数量（可以省略）
        job.setNumReduceTasks(mapReduceConfiguration.getNumReduceTasks());

        // 1.0:指定输入目录
        AvroParquetInputFormat.setInputPaths(job, new Path(mapReduceConfiguration.getInputPath()));
        // 1.8:指定输出目录
        if(!$.isEmptyOrNull(mapReduceConfiguration.getOutputPath()))
            FileOutputFormat.setOutputPath(job, new Path(mapReduceConfiguration.getOutputPath()));

        return this;
    }

    public $Hdfs getHdfs() {
        return hdfs;
    }

    public $MapReduce setHdfs($Hdfs hdfs) {
        this.hdfs = hdfs;
        return this;
    }

    public Job getJob() {
        return job;
    }

    public $MapReduce setJob(Job job) {
        this.job = job;
        return this;
    }

    public $MapReduceConfiguration getMapReduceConfiguration() {
        return mapReduceConfiguration;
    }

    public $MapReduce setMapReduceConfiguration($MapReduceConfiguration mapReduceConfiguration) {
        this.mapReduceConfiguration = mapReduceConfiguration;
        return this;
    }
}
