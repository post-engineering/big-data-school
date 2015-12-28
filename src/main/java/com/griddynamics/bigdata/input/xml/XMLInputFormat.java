package com.griddynamics.bigdata.input.xml;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * TODO
 */
public class XMLInputFormat extends TextInputFormat {

    public static final String CONF_XML_START_TAG = "xml.start.tag";
    public static final String CONF_XML_END_TAG = "xml.end.tag";

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new XMLRecordReader();
    }


}
