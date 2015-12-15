package com.griddynamics.bigdata.input.pdml;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by msigida on 12/15/15.
 * <p>
 * Just counts all bytes which go trough the job.
 * Used to verify that bytes read are equal to actual input size,
 * and no data was lost during split and/or compressed format handling.
 */
public class PDMLBytesCounter extends Mapper<LongWritable, BytesWritable, Text, LongWritable> {

    private static final Text KEY = new Text("bytes");
    private static final LongWritable VALUE = new LongWritable(0);

    @Override
    protected void map(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        VALUE.set(value.getLength());
        context.write(KEY, VALUE);
    }
}
