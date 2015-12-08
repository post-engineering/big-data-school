package com.griddynamics.bigdata;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by msigida on 11/24/15.
 */
public abstract class CountingJob implements Tool {

    private final static Logger LOG = LoggerFactory.getLogger(CountingJob.class);
    private final static String JAR_PATH = "target/bigdata-1.0-SNAPSHOT.jar";

    private Configuration conf;

    @Override
    public final int run(String[] args) throws Exception {
        LOG.info("Starting...");

        Job job = Job.getInstance(conf, getMapperClass().getCanonicalName());

        job.setJar(JAR_PATH);

        job.setJarByClass(CountingJob.class);
        job.setMapperClass(getMapperClass());
        job.setCombinerClass(CountingReducer.class);
        job.setReducerClass(CountingReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        LOG.info("Submitting job...");
        if (job.waitForCompletion(true)) {
            LOG.info("Job finished.");
        }

        return 0;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    public abstract Class<? extends Mapper> getMapperClass();
}
