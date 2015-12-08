package com.griddynamics.bigdata;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.StreamSupport;

/**
 * Created by msigida on 11/24/15.
 */
public class CountingReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = StreamSupport.stream(values.spliterator(), false).mapToInt(IntWritable::get).sum();
        context.write(key, new IntWritable(sum));
    }
}
