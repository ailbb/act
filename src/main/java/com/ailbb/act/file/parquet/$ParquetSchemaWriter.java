package com.ailbb.act.file.parquet;

import com.ailbb.act.$;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

/*
 * Created by Wz on 2017/12/29.
 */
public class $ParquetSchemaWriter {
    ParquetWriter<GenericRecord> writer;
    Schema schema;

    public $ParquetSchemaWriter(ParquetWriter<GenericRecord> writer, Schema schema){
        this.setWriter(writer);
        this.setSchema(schema);
    }

    public $ParquetSchemaWriter write(GenericRecord record) throws IOException {
        this.getWriter().write(record);
        return this;
    }

    public $ParquetSchemaWriter write(Object o) throws IOException, InvocationTargetException, IllegalAccessException {
        GenericRecord record = new GenericData.Record(this.getSchema());

        Class c = o.getClass();
        for(Field field : c.getDeclaredFields()) {
            try {
                Object val = $.bean.toCaseGetMethod(field, c).invoke(o);
                record.put(field.getName().toUpperCase(), val);
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            }
        }

        return this.write(record);
    }

    public $ParquetSchemaWriter write(List<Object> ol) throws IOException, InvocationTargetException, IllegalAccessException {
        for(Object o : ol) write(o);

        return this;
    }

    public void close() throws IOException {
        this.getWriter().close();
    }

    public ParquetWriter<GenericRecord> getWriter() {
        return writer;
    }

    public $ParquetSchemaWriter setWriter(ParquetWriter<GenericRecord> writer) {
        this.writer = writer;
        return this;
    }

    public Schema getSchema() {
        return schema;
    }

    public $ParquetSchemaWriter setSchema(Schema schema) {
        this.schema = schema;
        return this;
    }
}
