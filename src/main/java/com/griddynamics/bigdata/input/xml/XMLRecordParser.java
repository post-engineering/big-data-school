package com.griddynamics.bigdata.input.xml;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by msigida on 12/14/15.
 * <p>
 * Searches input for specified record boundary and stores captured record as byte array.
 */
public class XMLRecordParser implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(XMLRecordParser.class);
    private static final int MAX_RECORD_LENGTH = 1024 * 1024 * 10; // 10 Mb
    private final byte[] RECORD_START;
    private final byte[] RECORD_END;
    private final InputStream input;
    private final OpenByteArrayOutputStream out;
    private final RingBuffer cmp;

    private boolean moreDataNeeded = false;

    public XMLRecordParser(InputStream input, String openingTag, String closingTag) {
        this.input = input;
        this.RECORD_START = openingTag.getBytes();
        this.RECORD_END = closingTag.getBytes();
        this.out = new OpenByteArrayOutputStream(MAX_RECORD_LENGTH);
        this.cmp = new RingBuffer(Math.max(RECORD_START.length, RECORD_END.length));


    }

    @Override
    public void close() throws IOException {
        if (input != null) {
            input.close();
        }
    }

    // FIXME: I crave for refactoring... ba-a-adly...
    public boolean nextRecord() throws IOException {
        boolean foundStart = false;
        boolean shouldSkip = false;
        long skippedBytes = 0;
        int read;
        while ((read = input.read()) != -1) {
            cmp.add((byte) read);

            if (cmp.contains(RECORD_START)) {
                out.reset();
                out.write(RECORD_START);
                foundStart = true;
            } else if (cmp.contains(RECORD_END) && foundStart) {
                if (shouldSkip) {
                    foundStart = false;
                    shouldSkip = false;
                    LOG.warn("Skipped lengthy record for " + skippedBytes + " bytes.");
                    continue;
                }

                out.write(read);
                moreDataNeeded = false;
                return true;
            } else if (foundStart) {
                if (shouldSkip) {
                    skippedBytes++;
                } else {
                    out.write(read);
                }
            }

            if (out.size() >= MAX_RECORD_LENGTH && !shouldSkip) {
                shouldSkip = true;
                skippedBytes = MAX_RECORD_LENGTH;
                LOG.warn("Record length overflows maximum of " + MAX_RECORD_LENGTH + ". Skipping it...");
            }
        }

        if (foundStart) {
            moreDataNeeded = true;
            LOG.warn("Input stream ended prematurely: didn't found "
                    + new String(RECORD_END)
                    + " but already have read "
                    + getSize()
                    + " bytes.");
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

        public OpenByteArrayOutputStream(int size) {
            super(size);
        }

        public byte[] getUnderlying() {
            return buf;
        }
    }
}
