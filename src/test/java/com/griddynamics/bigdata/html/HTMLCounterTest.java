package com.griddynamics.bigdata.html;

import com.griddynamics.bigdata.PacketsToWordsJob;
import org.apache.hadoop.conf.Configuration;
import org.junit.Ignore;
import org.junit.Test;

import java.net.URISyntaxException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class HTMLCounterTest {

    private final String INPUT_ROOT_DIR = "testInput";
    private final String OUTPUT_ROOT_DIR = "testOutput";

    private String getAbsolutePathForFile(String fileName) throws URISyntaxException {
        return this.getClass().getClassLoader().getResource(fileName).toURI().toString();
    }

    @Test
    public void testJobLocally() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        conf.setInt("mapreduce.task.io.sort.mb", 1);

        PacketsToWordsJob job = new PacketsToWordsJob();
        job.setConf(conf);

        int exitCode = job.run(new String[]{
                "-i", getAbsolutePathForFile(INPUT_ROOT_DIR),
                "-o", getAbsolutePathForFile(OUTPUT_ROOT_DIR),
                "-c"
        });

        assertThat(exitCode, is(0));
        //TODO assert output

    }


    @Test
    @Ignore // TODO
    public void testJobOnCluster() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://172.26.5.36"); //active NN IP
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.address", "172.26.5.43:8032"); //active RM IP


        PacketsToWordsJob job = new PacketsToWordsJob();
        job.setConf(conf);
        int exitCode = job.run(new String[]{
                "-i", getAbsolutePathForFile(INPUT_ROOT_DIR),
                "-o", getAbsolutePathForFile(OUTPUT_ROOT_DIR),
                "-c"
        });

        assertThat(exitCode, is(0));
        //TODO assert output
    }
}
