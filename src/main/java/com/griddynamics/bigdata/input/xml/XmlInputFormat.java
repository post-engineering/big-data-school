package com.griddynamics.bigdata.input.xml;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * Input format for PDML records.
 * Splits big PDML files (possible for regular .xml and .bz2 formats), then parses it for each PDML packet record.
 * Mapper will receive record as XML document.
 */
public class XmlInputFormat extends TextInputFormat {

    public static final String CONF_XML_START_TAG = "xml.start.tag";
    public static final String CONF_XML_END_TAG = "xml.end.tag";

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new XmlRecordReader();
    }


}
