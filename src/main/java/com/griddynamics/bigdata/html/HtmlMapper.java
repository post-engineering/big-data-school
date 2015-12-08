package com.griddynamics.bigdata.html;

import com.griddynamics.bigdata.xml.XmlMapper;
import org.w3c.dom.Document;

import javax.xml.xpath.XPathExpressionException;

/**
 * Created by msigida on 11/24/15.
 */
public class HtmlMapper extends XmlMapper {

    @Override
    protected void mapXml(Document document, Context context) throws XPathExpressionException {
        String html = xPath.compile("//proto[@name='data-text-lines']/field").evaluate(document);
        // TODO: parse html
    }
}
