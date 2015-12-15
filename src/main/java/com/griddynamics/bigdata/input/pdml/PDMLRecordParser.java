package com.griddynamics.bigdata.input.pdml;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by msigida on 12/14/15.
 */
public class PDMLRecordParser implements Closeable {

    private static final byte[] RECORD_START = "<packet>".getBytes();
    private static final byte[] RECORD_END = "</packet>".getBytes();

    private final InputStream input;
    private final OpenByteArrayOutputStream out;
    private final RingBuffer cmp;

    private boolean moreDataNeeded = false;

    public PDMLRecordParser(InputStream input) {
        this.input = input;
        this.out = new OpenByteArrayOutputStream();
        this.cmp = new RingBuffer(Math.max(RECORD_START.length, RECORD_END.length));
    }

    @Override
    public void close() throws IOException {
        if (input != null) {
            input.close();
        }
    }

    public boolean nextRecord() throws IOException {
        boolean foundStart = false;
        int read;
        while ((read = input.read()) != -1) {
            cmp.add((byte) read);

            if (cmp.contains(RECORD_START)) {
                out.reset();
                out.write(RECORD_START);
                foundStart = true;
            } else if (cmp.contains(RECORD_END) && foundStart) {
                out.write(read);
                moreDataNeeded = false;
                return true;
            } else if (foundStart) {
                out.write(read);
            }
        }

        if (foundStart) {
            moreDataNeeded = true;
        }
        return false;
    }

    public byte[] getBytes() {
        return out.getUnderlying();
    }

    public int getSize() {
        return out.size();
    }

    public boolean isMoreDataNeeded() {
        return moreDataNeeded;
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
