package com.griddynamics.bigdata.input.pdml;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
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
import java.io.InputStream;
import java.io.StringWriter;

/**
 * Created by msigida on 12/9/15.
 */
public class PDMLRecordReader extends RecordReader<LongWritable, Text> {

    private static final Logger LOG = LoggerFactory.getLogger(PDMLRecordReader.class);
    private static final Integer BUFFER_SIZE = 1024 * 1024 * 32; // 32 MB

    private XMLEventReader reader;

    private LongWritable currentKey = new LongWritable(0);
    private Text currentValue;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) inputSplit;
        Path path = split.getPath();
        Configuration conf = context.getConfiguration();
        InputStream stream = getInputStream(path, conf);
        try {
            reader = XMLInputFactory.newInstance().createXMLEventReader(stream);
        } catch (XMLStreamException e) {
            LOG.error("Error creating XML reader", e);
            throw new IOException(e);
        }
    }

    private InputStream getInputStream(Path path, Configuration conf) throws IOException {
        FSDataInputStream stream = path.getFileSystem(conf).open(path);
        BufferedInputStream buffered;
        CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(path);
        if (codec != null) {
            Decompressor decompressor = CodecPool.getDecompressor(codec);
            CompressionInputStream compressed = codec.createInputStream(stream, decompressor);
            buffered = new BufferedInputStream(compressed, BUFFER_SIZE);
        } else {
            buffered = new BufferedInputStream(stream, BUFFER_SIZE);
        }
        return buffered;
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
        try {
            reader.close();
        } catch (XMLStreamException e) {
            throw new IOException("Error closing data stream", e);
        }
    }

    private String nextRecord() throws IOException {
        try {
            StringWriter output = new StringWriter();
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
