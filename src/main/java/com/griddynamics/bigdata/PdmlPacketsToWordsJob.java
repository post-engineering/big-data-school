package com.griddynamics.bigdata;


import com.griddynamics.bigdata.html.WordCounter;
import com.griddynamics.bigdata.html.WordExtractor;
import com.griddynamics.bigdata.input.xml.XmlInputFormat;
import com.griddynamics.bigdata.util.CustomJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This job converts extracted from PDML packets HTML to words and then counts them.
 */
@CustomJob
public class PdmlPacketsToWordsJob extends CustomizableJob {

    private final String XML_START_TAG = "<packet>";
    private final String XML_END_TAG = "</packet>";

    @Override
    public Class<? extends Mapper> getMapperClass() {
        return WordExtractor.class;
    }

    @Override
    public Class<? extends Reducer> getCombinerClass() {
        return SummingReducer.class;
    }

    @Override
    public Class<? extends Reducer> getReducerClass() {
        return WordCounter.class;
    }

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return XmlInputFormat.class;
    }

    @Override
    public Configuration getCustomConfiguration() {
        Configuration conf = new Configuration();
        conf.set(XmlInputFormat.CONF_XML_START_TAG, XML_START_TAG);
        conf.set(XmlInputFormat.CONF_XML_END_TAG, XML_END_TAG);
        return conf;
    }
}
