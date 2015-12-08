package com.griddynamics.bigdata.input;

import org.jsoup.Jsoup;
import org.jsoup.safety.Whitelist;

/**
 * The utility class for HTML processing
 */
public class HTMLPayloadProcessor {

    private HTMLPayloadProcessor(){
    }

    /**
     * Cleans the specified HTML up.
     * @param html structure to cleanup
     * @return well-formed html document
     */
    public static String cleanUpStructure(String html){
        return Jsoup.clean(html, Whitelist.relaxed());
    }

    /**
     * Extracts text from specified HTML even if its structure is broken.
     * @param html document
     * @return text payload
     */
    public static String extractTextSafely(String html){
        return  Jsoup.parse(cleanUpStructure(html)).text();
    }

}
