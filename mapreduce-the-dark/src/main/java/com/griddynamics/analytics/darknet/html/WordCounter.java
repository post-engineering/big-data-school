package com.griddynamics.analytics.darknet.html;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.StreamSupport;

/**
 * Counts values for each received key, but does not output low values.
 */
public class WordCounter extends Reducer<Text, LongWritable, Text, LongWritable> {

    private static final long MIN_COUNT = 10;

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = StreamSupport.stream(values.spliterator(), false).mapToLong(LongWritable::get).sum();
        if (sum >= MIN_COUNT) {
            context.write(key, new LongWritable(sum));
        }
    }
}
