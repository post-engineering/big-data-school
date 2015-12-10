package com.griddynamics.bigdata.xml;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * TODO
 */
public abstract class XMLMapper<KEYIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, Text, KEYOUT, VALUEOUT> {

    protected XPath xPath;
    protected DocumentBuilder builder;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            xPath = XPathFactory.newInstance().newXPath();
            builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            throw new IOException("Error creating XML document builder", e);
        }
    }

    @Override
    protected void map(KEYIN key, Text value, Context context) throws IOException, InterruptedException {
        InputStream stream = new ByteArrayInputStream(value.getBytes(), 0, value.getLength());
        try {
            Document document = builder.parse(stream);
            mapXml(document, key, context);
        } catch (SAXException | XPathExpressionException e) {
            throw new IOException("Error parsing provided XML", e);
        }
    }

    protected abstract void mapXml(Document document, KEYIN key, Context context) throws IOException, InterruptedException, XPathExpressionException;
}
