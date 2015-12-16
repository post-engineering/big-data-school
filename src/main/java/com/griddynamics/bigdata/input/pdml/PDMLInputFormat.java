package com.griddynamics.bigdata.input.pdml;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created by msigida on 12/9/15.
 * <p>
 * Input format for PDML records.
 * Splits big PDML files (possible for regular .xml and .bz2 formats), then parses it for each PDML packet record.
 * Mapper will receive record as XML document.
 */
public class PDMLInputFormat extends FileInputFormat<LongWritable, BytesWritable> {

    @Override
    public RecordReader<LongWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new PDMLRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        final CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return null == codec || codec instanceof SplittableCompressionCodec;
    }
}
