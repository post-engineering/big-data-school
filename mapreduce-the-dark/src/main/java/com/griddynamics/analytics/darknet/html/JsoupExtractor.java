package com.griddynamics.analytics.darknet.html;

import org.jsoup.Jsoup;
import org.jsoup.safety.Whitelist;

import java.io.IOException;
import java.nio.file.Path;

/**
 * The class is an implementation of {@link HtmlExtractor}
 * which uses Jsoup framework under the hood.
 */
public class JsoupExtractor implements HtmlExtractor {

    private String htmlPayload = null;

    public final static Whitelist WL_RELAXED = new Whitelist()
            .addTags(
                    "a", "b", "blockquote", "br", "caption", "cite",
                    "h1", "h2", "h3", "h4", "h5", "h6",
                    "i", "li", "ol", "p", "pre", "q", "small", "span", "strike", "strong",
                    "sub", "sup", "table", "tbody", "td", "tfoot", "th", "thead", "tr", "u",
                    "ul")

            .addAttributes("a", "href")
            .addProtocols("http", "https")
            .addProtocols("q", "cite", "http", "https");

    public static final Whitelist WL_STRONG = new Whitelist()
            .addTags("h1", "h2", "h3", "h4", "h5", "h6", "title",
                    "b", "em", "i", "strong", "u")
            .addProtocols("http", "https");

    public JsoupExtractor parseHtml(String pathToFile) throws IOException {
        htmlPayload = getHtml(pathToFile);
        return this;
    }


    public String extractTextSafely() {
        return extractText(cleanUpStructure(htmlPayload));
    }


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
        return cleanUpStructure(html, WL_STRONG);
    }

    @Override
    public String cleanUpStructure(String html, Whitelist wl) {
        return Jsoup.clean(html, wl);
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
