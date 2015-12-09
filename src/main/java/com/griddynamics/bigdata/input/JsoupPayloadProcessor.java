package com.griddynamics.bigdata.input;

import org.jsoup.Jsoup;
import org.jsoup.safety.Whitelist;

import java.io.IOException;
import java.nio.file.Path;

/**
 * The class is an implementation of {@link HTMLPayloadProcessor}
 * which uses Jsoup framework under the hood.
 */
public class JsoupPayloadProcessor implements HTMLPayloadProcessor {

    @Override
    public String getHtml(String url) throws IOException {
        return Jsoup.connect(url).get().html();
    }

    @Override
    public String getHtml(Path pathToFile) {
        return getHtml(pathToFile.toAbsolutePath());
    }

    @Override
    public String cleanUpStructure(String html) {
        return Jsoup.clean(html, Whitelist.relaxed());
    }

    @Override
    public String extractText(String html) {
        return Jsoup.parse(html).text();
    }


    @Override
    public String extractTextSafely(String html) {
        return extractText(cleanUpStructure(html));
    }

}
