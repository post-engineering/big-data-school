package com.griddynamics.analytics.darknet.util;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.bind.DatatypeConverter;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.*;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * The utility class provides common operations on PDML-format processing
 */
public class PdmlUtil {

    private final static String HTML_XPATH_QUERY = "//proto[@name='data-text-lines' and @showname='Line-based text data: text/html']/field[not(contains(@show,'text/javascript')) and not(contains(@show,'text/css'))]/@value";
    protected XPath xPath;
    protected DocumentBuilder builder;
    private XPathExpression expression;

    public PdmlUtil() throws IOException {
        try {
            xPath = XPathFactory.newInstance().newXPath();
            builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            expression = xPath.compile(HTML_XPATH_QUERY);
        } catch (ParserConfigurationException | XPathExpressionException e) {
            throw new IOException("Error creating XML document builder", e);
        }
    }

    public Document unmarshalizeXmlDocument(byte[] packetBytes, int offset, int length) throws IOException, InterruptedException {
        InputStream stream = new ByteArrayInputStream(packetBytes, offset, length);
        final Document document;
        try {
            document = builder.parse(stream);
        } catch (SAXException e) {
            throw new IOException("Error parsing provided XML", e);
        }

        return document;

    }

    public String extractHTMLPayloadFromPacket(byte[] packetBytes, int offset, int length) throws IOException,
            InterruptedException,
            XPathExpressionException {

        Document document = unmarshalizeXmlDocument(packetBytes, offset, length);
        String html = extractHtmlPayloadFromPacket(document);
        return html;
    }


    public String extractHtmlPayloadFromPacket(Document document) throws XPathExpressionException {
        NodeList values = (NodeList) expression.evaluate(document, XPathConstants.NODESET);
        if (values.getLength() == 0) {
            return null;

        }
        StringBuilder hexHtmlBuilder = new StringBuilder();
        for (int i = 0; i < values.getLength(); ++i) {
            hexHtmlBuilder.append(values.item(i).getNodeValue());
        }
        byte[] html = DatatypeConverter.parseHexBinary(hexHtmlBuilder.toString());
        return new String(html, StandardCharsets.UTF_8);
    }

}
