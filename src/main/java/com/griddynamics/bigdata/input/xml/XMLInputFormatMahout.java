package com.griddynamics.bigdata.input.xml;

import com.google.common.io.Closeables;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * TODO
 */
public class XMLInputFormatMahout extends TextInputFormat {

    public static final String START_TAG_KEY = "xmlinput.start";
    public static final String END_TAG_KEY = "xmlinput.end";
    private static final Logger log = LoggerFactory.getLogger(XMLInputFormatMahout.class);

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        try {
            return new XmlRecordReader((FileSplit) split, context.getConfiguration());
        } catch (IOException ioe) {
            log.warn("Error while creating XmlRecordReader", ioe);
            return null;
        }
    }

    /**
     * XMLRecordReader class to read through a given xml document to output xml blocks as records as specified
     * by the start tag and end tag
     */
    public static class XmlRecordReader extends RecordReader<LongWritable, Text> {

        private final byte[] startTag;
        private final byte[] endTag;
        private final InputStream fsin;
        private final DataOutputBuffer buffer = new DataOutputBuffer();
        private long start;
        private long end;
        private LongWritable currentKey;
        private Text currentValue;

        private Decompressor decompressor;
        private Seekable position;

        public XmlRecordReader(FileSplit split, Configuration conf) throws IOException {
            startTag = conf.get(START_TAG_KEY).getBytes(Charsets.UTF_8);
            endTag = conf.get(END_TAG_KEY).getBytes(Charsets.UTF_8);

            // open the file and seek to the start of the split
            start = split.getStart();
            end = start + split.getLength();
            Path file = split.getPath();

            fsin = getInputStream(file, conf);

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


        private boolean next(LongWritable key, Text value) throws IOException {
            if (position.getPos() < end && readUntilMatch(startTag, false)) {
                try {
                    buffer.write(startTag);
                    if (readUntilMatch(endTag, true)) {
                        key.set(position.getPos());
                        value.set(buffer.getData(), 0, buffer.getLength());
                        return true;
                    }
                } finally {
                    buffer.reset();
                }
            }
            return false;
        }

        @Override
        public void close() throws IOException {
            Closeables.close(fsin, true);
        }

        @Override
        public float getProgress() throws IOException {
            return (position.getPos() - start) / (float) (end - start);
        }

        private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
            int i = 0;
            while (true) {
                int b = fsin.read();
                // end of file:
                if (b == -1) {
                    return false;
                }
                // save to buffer:
                if (withinBlock) {
                    buffer.write(b);
                }

                // check if we're matching:
                if (b == match[i]) {
                    i++;
                    if (i >= match.length) {
                        return true;
                    }
                } else {
                    i = 0;
                }
                // see if we've passed the stop point:
                if (!withinBlock && i == 0 && position.getPos() >= end) {
                    return false;
                }
            }
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
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            currentKey = new LongWritable();
            currentValue = new Text();
            return next(currentKey, currentValue);
        }
    }
}
