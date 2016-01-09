package com.griddynamics.analytics.darknet.html;

import com.griddynamics.analytics.darknet.util.PdmlUtil;
import com.griddynamics.analytics.darknet.xml.XMLMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.w3c.dom.Document;

import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.text.BreakIterator;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Extracts HTML from PDML packets and then parses it to words.
 * Outputs each word as the key and '1' as the value.
 */
public class WordExtractor extends XMLMapper<LongWritable, Text, LongWritable> {

    private final static Integer MIN_WORD_LENGTH = 3;
    private final static LongWritable ONE = new LongWritable(1);

    private HtmlExtractor extractor;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        extractor = new JsoupExtractor();
    }

    @Override
    protected void mapXml(Document document, LongWritable key, Context context) throws IOException, InterruptedException, XPathExpressionException {
        String html = new PdmlUtil().extractHtmlPayloadFromPacket(document);
        if (html != null) {
            for (String word : extractWords(html)) {
                if (word.length() >= MIN_WORD_LENGTH) {
                    context.write(new Text(word), ONE);
                }
            }
        }
    }

    private Iterable<String> extractWords(String html) {
        String text = extractor.extractTextSafely(html);
        return new WordIterable(text);
    }

    private class WordIterable implements Iterable<String> {

        private final String text;
        private final BreakIterator breakIterator;

        public WordIterable(String text) {
            this.text = text;
            this.breakIterator = BreakIterator.getWordInstance();

            breakIterator.setText(text);
        }

        @Override
        public Iterator<String> iterator() {
            return new WordIterator();
        }

        private class WordIterator implements Iterator<String> {

            int wordBoundaryIndex = breakIterator.first();
            int prevIndex = 0;

            @Override
            public boolean hasNext() {
                return wordBoundaryIndex != BreakIterator.DONE;
            }

            @Override
            public String next() {
                if (hasNext()) {
                    String word = text.substring(prevIndex, wordBoundaryIndex);
                    prevIndex = wordBoundaryIndex;
                    wordBoundaryIndex = breakIterator.next();
                    return word.toLowerCase();
                }
                throw new NoSuchElementException();
            }
        }
    }
}
