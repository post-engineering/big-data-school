package com.griddynamics.bigdata.html;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@RunWith(JUnit4.class)
public class HTMLCounterTest {

    private static final HTMLMapper mapper = new HTMLMapper();

    //@Test TODO
    public void testJobMapper() {
        //new MapDriver<LongWritable,Text,IntWritable>().withMapper(mapper).runTest();
    }

    @Test
    public void testJob() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        conf.setInt("mapreduce.task.io.sort.mb", 1);

        Path input = new Path("input");
        Path output = new Path("output");

        HTMLCounterJob job = new HTMLCounterJob();
        job.setConf(conf);

        int exitCode = job.run(new String[]{
                "-j", "HTMLCounterJob",
                "-i", input.toString(),
                "-o", output.toString(),
                "-c"
        });

        assertThat(exitCode, is(0));
        //TODO assert output

    }
}
