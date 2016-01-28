package com.griddynamics.analytics.darknet.html;

import org.jsoup.safety.Whitelist;
import java.io.IOException;
import java.nio.file.Path;

/**
 * The interface declares custom HTML processing functionality.
 */
public interface HtmlExtractor {

    /**
     * Gets HTML document from the specified url
     *
     * @param url ex. : www.google.com
     * @return HTML doument as a String
     * @throws IOException
     */
    String getHtml(String url) throws IOException;


    /**
     * Gets HTML document from the specified file
     *
     * @param pathToFile
     * @return HTML doument as a String
     * @throws IOException
     */
    String getHtml(Path pathToFile) throws IOException;

    /**
     * Cleans the specified HTML up.
     *
     * @param html structure to cleanup
     * @return well-formed html document
     */
    String cleanUpStructure(String html);

    /**
     * Cleans the specified HTML up by using specific {@link Whitelist} implementation.
     * @param html html structure to cleanup
     * @param wl filtering requirements holder
     * @return
     */
    String cleanUpStructure(String html, Whitelist wl);


    /**
     * Extracts text from specified HTML.
     *
     * @param html document
     * @return text payload
     */
    String extractText(String html);

    /**
     * Extracts text from specified HTML even if its structure is broken.
     *
     * @param html document
     * @return text payload
     */
    String extractTextSafely(String html);

}
