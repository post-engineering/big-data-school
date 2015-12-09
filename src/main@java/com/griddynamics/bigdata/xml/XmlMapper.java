package com.griddynamics.bigdata.xml;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 * Created by msigida on 11/24/15.
 */
public abstract class XmlMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static Logger LOG = LoggerFactory.getLogger(XmlMapper.class);

    protected final static IntWritable ONE = new IntWritable(1);

    protected DocumentBuilderFactory builderFactory;
    protected XPath xPath;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        builderFactory = DocumentBuilderFactory.newInstance();
        xPath = XPathFactory.newInstance().newXPath();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        InputStream stream = new ByteArrayInputStream(value.getBytes(), 0, value.getLength());
        try {
            DocumentBuilder builder = builderFactory.newDocumentBuilder();
            Document document = builder.parse(stream);
            mapXml(document, context);
        } catch (ParserConfigurationException | SAXException | XPathExpressionException e) {
            LOG.error("Error parsing provided XML", e);
        }
    }

    protected abstract void mapXml(Document document, Context context) throws XPathExpressionException;
}
