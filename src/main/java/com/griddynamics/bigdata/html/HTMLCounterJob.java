package com.griddynamics.bigdata.html;


import com.griddynamics.bigdata.CountingReducer;
import com.griddynamics.bigdata.CustomizableJob;
import com.griddynamics.bigdata.framework.CustomJob;
import com.griddynamics.bigdata.input.pdml.PDMLInputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * TODO
 */
@CustomJob
public class HTMLCounterJob extends CustomizableJob {

    @Override
    public Class<? extends Mapper> getMapperClass() {
        return HTMLMapper.class;
    }

    @Override
    public Class<? extends Reducer> getCombinerClass() {
        return CountingReducer.class;
    }

    @Override
    public Class<? extends Reducer> getReducerClass() {
        return CountingReducer.class;
    }

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return PDMLInputFormat.class;
    }
}
