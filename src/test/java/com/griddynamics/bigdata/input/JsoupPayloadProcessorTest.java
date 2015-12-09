package com.griddynamics.bigdata.input;

import com.griddynamics.bigdata.html.HTMLProcessor;
import com.griddynamics.bigdata.html.JsoupProcessor;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tests for {@link JsoupProcessor} class
 */
@RunWith(JUnit4.class)
public class JsoupPayloadProcessorTest {

    private final HTMLProcessor htmlProcessor = new JsoupProcessor();
     /*
      has no closing <p> tag
      */
    private final String DIRTY_HTML_1 = "<p>First Part<ul><li>list item 1</li><li>list item 2</li></ul> second part";

    private final String PORNOHUB_URL = "http://www.pornhub.com/categories";
    private final Pattern HTML_TAG_PATTERN = Pattern.compile("<(\"[^\"]*\"|'[^']*'|[^'\">])*>");


    @Test
    public void testExtractTextFromDirtyHTML(){
        String actual = htmlProcessor.extractTextSafely(DIRTY_HTML_1);
        String expected = "First Part list item 1 list item 2 second part";
        Assert.assertEquals(actual, expected);
    }


    @Test
    public void testExtractTextFromBigHTML() throws IOException {

        String bidHtml = htmlProcessor.getHtml(PORNOHUB_URL);
        Matcher matcher = HTML_TAG_PATTERN.matcher(bidHtml);
        Assert.assertTrue(matcher.find());

        String actual = htmlProcessor.extractTextSafely(bidHtml);
        matcher = HTML_TAG_PATTERN.matcher(actual);
        Assert.assertFalse(matcher.find());
    }
}
