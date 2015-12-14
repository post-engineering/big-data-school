package com.griddynamics.bigdata;


import com.griddynamics.bigdata.util.ExtendedOptionsParser;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO
 */
public abstract class CustomizableJob extends Configured implements Tool {

    public static final String ROOT_PACKAGE = "com.griddynamics.bigdata";
    private final static Logger LOG = LoggerFactory.getLogger(CustomizableJob.class);

    private ExtendedOptionsParser optionsParser;

    public static CustomizableJob parseJob(String[] args) throws Exception {
        ExtendedOptionsParser optionsParser = new ExtendedOptionsParser(ROOT_PACKAGE, args);
        return optionsParser.parseJob();
    }

    @Override
    public final int run(String[] args) throws Exception {
        try {
            optionsParser = new ExtendedOptionsParser(ROOT_PACKAGE, args);
        } catch (ParseException e) {
            LOG.error("Please specify valid input parameters");
            LOG.error(optionsParser.getExtendedOptionsUsage());
            return -1;
        }

        LOG.info("Starting...");

        Job job = Job.getInstance(getConf(), getMapperClass().getCanonicalName());

        job.setJarByClass(CustomizableJob.class);
        job.setMapperClass(getMapperClass());
        job.setCombinerClass(getCombinerClass());
        job.setReducerClass(getReducerClass());

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(getInputFormatClass());

        Path inputPath = optionsParser.getInputPath();
        Path outputPath = optionsParser.getOutputPath();
        FileInputFormat.addInputPath(job, inputPath);
        if (optionsParser.getCleanOutput()) {
            FileSystem.get(getConf()).delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * TODO
     *
     * @return
     */
    public abstract Class<? extends Mapper> getMapperClass();

    /**
     * TODO
     *
     * @return
     */
    public abstract Class<? extends Reducer> getCombinerClass();

    /**
     * TODO
     *
     * @return
     */
    public abstract Class<? extends Reducer> getReducerClass();

    /**
     * TODO
     *
     * @return
     */
    public abstract Class<? extends org.apache.hadoop.mapreduce.InputFormat> getInputFormatClass();

    //TODO Output/Input format?
}
