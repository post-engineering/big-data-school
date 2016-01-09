package com.griddynamics.bigdata.input.xml;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.io.InputStream;

/**
 * The {@link RecordReader} implementation can be applied for reading content of particular XML-node.
 */
public class XmlRecordReader extends RecordReader<LongWritable, Text> {

    private long start;
    private long end;

    private Decompressor decompressor;
    private Seekable position;

    private LongWritable currentKey = new LongWritable(0);
    private Text currentValue = new Text();
    private XmlRecordParser parser;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) inputSplit;
        start = split.getStart();
        end = split.getStart() + split.getLength();

        Path path = split.getPath();
        Configuration conf = context.getConfiguration();
        InputStream input = getInputStream(path, conf);

        String openingTag = conf.get(XmlInputFormat.CONF_XML_NODE_START_TAG);
        String closingTag = conf.get(XmlInputFormat.CONF_XML_NODE_END_TAG);

        if ((openingTag == null || openingTag.isEmpty()) ||
                (closingTag == null || closingTag.isEmpty())) {
            throw new IOException(
                    String.format("You must provide the job's configuration with \"%s\" / \"%s\" parameters!",
                            XmlInputFormat.CONF_XML_NODE_START_TAG,
                            XmlInputFormat.CONF_XML_NODE_END_TAG
                    )
            );
        }
        parser = new XmlRecordParser(input, openingTag, closingTag);
    }

    private InputStream getInputStream(Path path, Configuration conf) throws IOException {
        FSDataInputStream stream = path.getFileSystem(conf).open(path);
        CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(path);
        if (codec != null) {
            decompressor = CodecPool.getDecompressor(codec);
            if (codec instanceof SplittableCompressionCodec) {
                final SplitCompressionInputStream splittable = ((SplittableCompressionCodec) codec)
                        .createInputStream(stream, decompressor, start, end,
                                SplittableCompressionCodec.READ_MODE.BYBLOCK);
                start = splittable.getAdjustedStart();
                end = splittable.getAdjustedEnd();
                position = splittable;
                return splittable;
            } else {
                CompressionInputStream compressed = codec.createInputStream(stream, decompressor);
                position = compressed;
                return compressed;
            }
        } else {
            stream.seek(start);
            position = stream;
            return stream;
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (position.getPos() <= end && parser.nextRecord()) {
            currentKey.set(currentKey.get() + 1);
            currentValue.set(parser.getBytes(), 0, parser.getSize());
            return true;
        }

        currentKey = null;
        currentValue = null;
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
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (position.getPos() - start) / (float) (end - start));
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (parser != null) {
                parser.close();
            }
        } finally {
            if (decompressor != null) {
                CodecPool.returnDecompressor(decompressor);
                decompressor = null;
            }
        }
    }
}
