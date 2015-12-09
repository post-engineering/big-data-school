package com.griddynamics.bigdata.input;

/**
 * The interface declares custom HTML processing functionality
 */
public interface HTMLPayloadProcessor {

    /**
     * Cleans the specified HTML up.
     *
     * @param html structure to cleanup
     * @return well-formed html document
     */
    String cleanUpStructure(String html);


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
