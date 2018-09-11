package com.ailbb.act.entity;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Wz on 9/10/2018.
 */
public class $MapReduceConfiguration {
    private String jobName;
    private Class jarClass;
    private Class mapOutputKeyClass = Text.class;
    private Class mapOutputValueClass = Text.class;
    private Class reduceOutputKeyClass = Void.class;
    private Class reduceOutputValueClass = Void.class;
    private Class<? extends InputFormat>  inputFormatClass = AvroParquetInputFormat.class;
    private Class<? extends OutputFormat> outputFormatClass = NullOutputFormat.class;
    private Class<? extends Mapper> mapper;
    private Class<? extends Reducer> reducer;
    private int numReduceTasks = 1;

    private List<String> jarPath = new ArrayList<>();
    private String inputPath;
    private String outputPath;

    public String getJobName() {
        return jobName;
    }

    public $MapReduceConfiguration setJobName(String jobName) {
        this.jobName = jobName;
        return this;
    }

    public Class getJarClass() {
        return jarClass;
    }

    public $MapReduceConfiguration setJarClass(Class jarClass) {
        this.jarClass = jarClass;
        return this;
    }

    public Class getMapOutputKeyClass() {
        return mapOutputKeyClass;
    }

    public $MapReduceConfiguration setMapOutputKeyClass(Class mapOutputKeyClass) {
        this.mapOutputKeyClass = mapOutputKeyClass;
        return this;
    }

    public Class getMapOutputValueClass() {
        return mapOutputValueClass;
    }

    public $MapReduceConfiguration setMapOutputValueClass(Class mapOutputValueClass) {
        this.mapOutputValueClass = mapOutputValueClass;
        return this;
    }

    public Class getReduceOutputKeyClass() {
        return reduceOutputKeyClass;
    }

    public $MapReduceConfiguration setReduceOutputKeyClass(Class reduceOutputKeyClass) {
        this.reduceOutputKeyClass = reduceOutputKeyClass;
        return this;
    }

    public Class getReduceOutputValueClass() {
        return reduceOutputValueClass;
    }

    public $MapReduceConfiguration setReduceOutputValueClass(Class reduceOutputValueClass) {
        this.reduceOutputValueClass = reduceOutputValueClass;
        return this;
    }

    public Class<? extends InputFormat> getInputFormatClass() {
        return inputFormatClass;
    }

    public $MapReduceConfiguration setInputFormatClass(Class<? extends InputFormat> inputFormatClass) {
        this.inputFormatClass = inputFormatClass;
        return this;
    }

    public Class<? extends OutputFormat> getOutputFormatClass() {
        return outputFormatClass;
    }

    public $MapReduceConfiguration setOutputFormatClass(Class<? extends OutputFormat> outputFormatClass) {
        this.outputFormatClass = outputFormatClass;
        return this;
    }

    public Class<? extends Mapper> getMapper() {
        return mapper;
    }

    public $MapReduceConfiguration setMapper(Class<? extends Mapper> mapper) {
        this.mapper = mapper;
        return this;
    }

    public Class<? extends Reducer> getReducer() {
        return reducer;
    }

    public $MapReduceConfiguration setReducer(Class<? extends Reducer> reducer) {
        this.reducer = reducer;
        return this;
    }

    public int getNumReduceTasks() {
        return numReduceTasks;
    }

    public $MapReduceConfiguration setNumReduceTasks(int numReduceTasks) {
        this.numReduceTasks = numReduceTasks;
        return this;
    }

    public List<String> getJarPath() {
        return jarPath;
    }

    public $MapReduceConfiguration setJarPath(List<String> jarPath) {
        this.jarPath = jarPath;
        return this;
    }

    public String getInputPath() {
        return inputPath;
    }

    public $MapReduceConfiguration setInputPath(String inputPath) {
        this.inputPath = inputPath;
        return this;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public $MapReduceConfiguration setOutputPath(String outputPath) {
        this.outputPath = outputPath;
        return this;
    }
}
