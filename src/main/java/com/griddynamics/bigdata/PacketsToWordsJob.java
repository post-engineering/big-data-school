package com.griddynamics.bigdata;


import com.griddynamics.bigdata.html.WordCounter;
import com.griddynamics.bigdata.html.WordExtractor;
import com.griddynamics.bigdata.input.pdml.PDMLInputFormat;
import com.griddynamics.bigdata.util.CustomJob;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This job converts extracted from PDML packets HTML to words and then counts them.
 */
@CustomJob
public class PacketsToWordsJob extends CustomizableJob {

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
        return PDMLInputFormat.class;
    }
}
