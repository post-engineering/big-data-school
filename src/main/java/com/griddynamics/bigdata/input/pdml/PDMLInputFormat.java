package com.griddynamics.bigdata.input.pdml;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created by msigida on 12/9/15.
 */
public class PDMLInputFormat extends FileInputFormat<LongWritable, BytesWritable> {

    @Override
    public RecordReader<LongWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new PDMLRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}
