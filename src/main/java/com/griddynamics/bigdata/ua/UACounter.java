package com.griddynamics.bigdata.ua;


import com.griddynamics.bigdata.CustomizableJob;
import com.griddynamics.bigdata.util.CustomJob;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * TODO
 */
@CustomJob
public class UACounter extends CustomizableJob {

    @Override
    public Class<? extends Mapper> getMapperClass() {
        return UAMapper.class;
    }

    @Override
    public Class<? extends Reducer> getCombinerClass() {
        return null;
    }

    @Override
    public Class<? extends Reducer> getReducerClass() {
        return null;
    }

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return null;
    }
}
