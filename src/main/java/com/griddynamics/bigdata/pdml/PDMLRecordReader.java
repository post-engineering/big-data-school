package com.griddynamics.bigdata.pdml;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.*;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.StringWriter;

/**
 * Created by msigida on 12/9/15.
 */
public class PDMLRecordReader extends RecordReader<LongWritable, Text> {

    private static final Logger LOG = LoggerFactory.getLogger(PDMLRecordReader.class);
    private static final Integer BUFF_SIZE = 1024 * 32;

    private FSDataInputStream file;
    private Long start;
    private Long end;

    private XMLEventReader reader;

    private LongWritable currentKey = new LongWritable(0);
    private Text currentValue;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) inputSplit;
        Path path = split.getPath();
        FileSystem fs = path.getFileSystem(context.getConfiguration());
        file = fs.open(path);
        start = split.getStart();
        end = start + split.getLength();

        BufferedInputStream buffered = new BufferedInputStream(file, BUFF_SIZE);
        XMLInputFactory factory = XMLInputFactory.newInstance();
        try {
            reader = factory.createXMLEventReader(buffered);
        } catch (XMLStreamException e) {
            LOG.error("Error creating XML reader", e);
            throw new IOException(e);
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        String record = nextRecord();
        if (record != null) {
            currentKey = new LongWritable(currentKey.get() + 1);
            currentValue = new Text(record);
            return true;
        }
        return false;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        file.close();
    }

    private String nextRecord() throws IOException {
        try {
            StringWriter output = new StringWriter(BUFF_SIZE);
            XMLEventWriter writer = null;
            while (reader.hasNext()) {
                XMLEvent e = reader.nextEvent();
                if (e.isStartElement() && ((StartElement) e).getName().getLocalPart().equals("packet")) {
                    writer = XMLOutputFactory.newInstance().createXMLEventWriter(output);
                    writer.add(e);
                } else if (e.isEndElement() && ((EndElement) e).getName().getLocalPart().equals("packet")) {
                    if (writer != null) {
                        writer.add(e);
                        writer.flush();
                        return output.toString();
                    }
                    return null;
                } else if (writer != null) {
                    writer.add(e);
                }
            }
            return null;
        } catch (XMLStreamException e) {
            LOG.error("Error during XML parsing", e);
            throw new IOException(e);
        }
    }
}
