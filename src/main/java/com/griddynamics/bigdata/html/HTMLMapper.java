package com.griddynamics.bigdata.html;

import com.griddynamics.bigdata.xml.XMLMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import javax.xml.bind.DatatypeConverter;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.BreakIterator;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * TODO
 */
public class HTMLMapper extends XMLMapper<LongWritable, Text, LongWritable> {

    private final static String HTML_XPATH_QUERY = "//proto[@name='data-text-lines']/field/@value";
    private final static Integer MIN_WORD_LENGTH = 3;
    private final static LongWritable ONE = new LongWritable(1);

    private HTMLProcessor processor;
    private XPathExpression expression;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        try {
            processor = new JsoupProcessor();
            expression = xPath.compile(HTML_XPATH_QUERY);
        } catch (XPathExpressionException e) {
            throw new IOException("Error parsing xPath expression: " + HTML_XPATH_QUERY, e);
        }
    }

    @Override
    protected void mapXml(Document document, LongWritable key, Context context) throws IOException, InterruptedException, XPathExpressionException {
        String html = extractHtml(document);
        if (html != null) {
            for (String word : extractWords(html)) {
                if (word.length() >= MIN_WORD_LENGTH) {
                    context.write(new Text(word), ONE);
                }
            }
        }
    }

    private String extractHtml(Document document) throws XPathExpressionException {
        NodeList values = (NodeList) expression.evaluate(document, XPathConstants.NODESET);
        if (values.getLength() == 0) {
            return null;
        }
        StringBuilder hexHtmlBuilder = new StringBuilder();
        for (int i = 0; i < values.getLength(); ++i) {
            hexHtmlBuilder.append(values.item(i).getNodeValue());
        }
        byte[] html = DatatypeConverter.parseHexBinary(hexHtmlBuilder.toString());
        return new String(html, StandardCharsets.UTF_8);
    }

    private Iterable<String> extractWords(String html) {
        String text = processor.extractTextSafely(html);
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
