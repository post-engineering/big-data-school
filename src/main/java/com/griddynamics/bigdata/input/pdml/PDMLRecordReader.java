package com.griddynamics.bigdata.input.pdml;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by msigida on 12/9/15.
 */
public class PDMLRecordReader extends RecordReader<LongWritable, BytesWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(PDMLRecordReader.class);

    private static final byte[] RECORD_START = "<packet>".getBytes();
    private static final byte[] RECORD_END = "</packet>".getBytes();
    private static final int BUFFER_SIZE = 1024 * 1024 * 4; // 4 MB

    private InputStream input;

    private LongWritable currentKey = new LongWritable(0);
    private BytesWritable currentValue;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) inputSplit;
        Path path = split.getPath();
        Configuration conf = context.getConfiguration();
        input = getInputStream(path, conf);
    }

    private InputStream getInputStream(Path path, Configuration conf) throws IOException {
        FSDataInputStream stream = path.getFileSystem(conf).open(path);
        CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(path);
        BufferedInputStream buffered;
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
        BytesWritable record = nextRecord();
        if (record != null) {
            currentKey = new LongWritable(currentKey.get() + 1);
            currentValue = record;
            return true;
        }
        return false;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        input.close();
    }

    private BytesWritable nextRecord() throws IOException {
        final RingBuffer cmp = new RingBuffer(Math.max(RECORD_START.length, RECORD_END.length));
        OpenByteArrayOutputStream out = null;

        int read;
        while ((read = input.read()) != -1) {
            cmp.add((byte) read);

            if (cmp.contains(RECORD_START)) {
                out = new OpenByteArrayOutputStream();
                out.write(RECORD_START);
            } else if (cmp.contains(RECORD_END) && out != null) {
                out.write(read);
                return new BytesWritable(out.getUnderlying(), out.size());
            } else if (out != null) {
                out.write(read);
            }
        }
        return null;
    }

    // FIXME: currently using shift to rotate the buffer -> super inefficient
    private class RingBuffer {

        private final byte[] buffer;

        public RingBuffer(int size) {
            this.buffer = new byte[size];
        }

        public void add(byte b) {
            System.arraycopy(buffer, 0, buffer, 1, buffer.length - 1);
            buffer[0] = b;
        }

        private boolean contains(byte[] subset) {
            if (buffer.length < subset.length) {
                return false;
            }
            for (int i = 0; i < subset.length; i++) {
                if (subset[i] != buffer[buffer.length - 1 - i]) {
                    return false;
                }
            }
            return true;
        }
    }

    private class OpenByteArrayOutputStream extends ByteArrayOutputStream {

        public byte[] getUnderlying() {
            return buf;
        }
    }
}
