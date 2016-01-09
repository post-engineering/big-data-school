package com.griddynamics.analytics.darknet.ua;

import com.griddynamics.analytics.darknet.xml.XMLMapper;
import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.service.UADetectorServiceFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.w3c.dom.Document;

import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;

/**
 * Created by msigida on 11/24/15.
 */
public class UaMapper extends XMLMapper<LongWritable, Text, LongWritable> {

    private UserAgentStringParser parser;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        parser = UADetectorServiceFactory.getResourceModuleParser();
    }

    @Override
    protected void mapXml(Document document, LongWritable key, Context context) throws XPathExpressionException {
        // TODO: parse user agent
    }
}
