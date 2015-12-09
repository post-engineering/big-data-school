package com.griddynamics.bigdata.html;

import org.jsoup.Jsoup;
import org.jsoup.safety.Whitelist;

import java.io.IOException;
import java.nio.file.Path;

/**
 * The class is an implementation of {@link HTMLProcessor}
 * which uses Jsoup framework under the hood.
 */
public class JsoupProcessor implements HTMLProcessor {

    @Override
    public String getHtml(String url) throws IOException {
        return Jsoup.connect(url).get().html();
    }

    @Override
    public String getHtml(Path pathToFile) throws IOException {
        return getHtml(pathToFile.toAbsolutePath().toString());
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
