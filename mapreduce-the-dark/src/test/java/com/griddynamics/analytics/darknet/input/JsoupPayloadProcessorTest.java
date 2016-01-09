package com.griddynamics.analytics.darknet.input;

import com.griddynamics.analytics.darknet.html.HtmlExtractor;
import com.griddynamics.analytics.darknet.html.JsoupExtractor;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tests for {@link JsoupExtractor} class
 */
public class JsoupPayloadProcessorTest {

    private final HtmlExtractor htmlExtractor = new JsoupExtractor();
    /*
     has no closing <p> tag
     */
    private final String DIRTY_HTML_1 = "<p>First Part<ul><li>list item 1</li><li>list item 2</li></ul> second part";

    private final String PORNOHUB_URL = "http://www.pornhub.com/categories";
    private final Pattern HTML_TAG_PATTERN = Pattern.compile("<(\"[^\"]*\"|'[^']*'|[^'\">])*>");

    @Ignore
    @Test
    public void testExtractTextFromDirtyHTML() {
        String actual = htmlExtractor.extractTextSafely(DIRTY_HTML_1);
        String expected = "First Part list item 1 list item 2 second part";
        Assert.assertEquals(actual, expected);
    }


    @Ignore
    @Test
    public void testExtractTextFromBigHTML() throws IOException {
        String bidHtml = htmlExtractor.getHtml(PORNOHUB_URL);
        Matcher matcher = HTML_TAG_PATTERN.matcher(bidHtml);
        Assert.assertTrue(matcher.find());

        String actual = htmlExtractor.extractTextSafely(bidHtml);
        matcher = HTML_TAG_PATTERN.matcher(actual);
        Assert.assertFalse(matcher.find());
    }

}
