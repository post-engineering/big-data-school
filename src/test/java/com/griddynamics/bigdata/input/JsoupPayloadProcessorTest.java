package com.griddynamics.bigdata.input;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link JsoupPayloadProcessor} class
 */
@RunWith(JUnit4.class)
public class JsoupPayloadProcessorTest {

    private final HTMLPayloadProcessor htmlPayloadProcessor = new JsoupPayloadProcessor();
     /*
      has no closing <p> tag
      */
    private final String DIRTY_HTML_1 = "<p>First Part<ul><li>list item 1</li><li>list item 2</li></ul> second part";


    @Test
    public void testExtractTextFromDirtyHTML(){
        String actual = htmlPayloadProcessor.extractTextSafely(DIRTY_HTML_1);
        String expected = "First Part list item 1 list item 2 second part";
        Assert.assertEquals(actual, expected);
    }
}
