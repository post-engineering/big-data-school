package com.griddynamics.bigdata.input;

import org.jsoup.Jsoup;
import org.jsoup.safety.Whitelist;

/**
 * The class is an implementation of {@link HTMLPayloadProcessor}
 * which uses Jsoup framework under the hood.
 */
public class JsoupPayloadProcessor implements HTMLPayloadProcessor {

    /**
     * Cleans the specified HTML up.
     *
     * @param html structure to cleanup
     * @return well-formed html document
     */
    @Override
    public String cleanUpStructure(String html) {
        return Jsoup.clean(html, Whitelist.relaxed());
    }

    @Override
    public String extractText(String html) {
        return Jsoup.parse(html).text();
    }

    /**
     * Extracts text from specified HTML even if its structure is broken.
     *
     * @param html document
     * @return text payload
     */
    @Override
    public String extractTextSafely(String html) {
        return extractText(cleanUpStructure(html));
    }

}
