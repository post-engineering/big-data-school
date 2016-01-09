package com.griddynamics.analytics.darknet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.StreamSupport;

/**
 * Created by msigida on 11/24/15.
 * <p>
 * Just sums up all values for each key.
 */
public class SummingReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = StreamSupport.stream(values.spliterator(), false).mapToLong(LongWritable::get).sum();
        context.write(key, new LongWritable(sum));
    }
}
