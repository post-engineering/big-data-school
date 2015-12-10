package com.griddynamics.bigdata.framework;


import com.griddynamics.bigdata.CustomizableJob;
import org.apache.hadoop.util.ToolRunner;

/**
 * TODO
 */
public class JobRunner {

    public static void main(String[] args) throws Exception {
        CustomizableJob job = CustomizableJob.parseJob(args);
        if (job == null) {
            System.err.printf("Specified custom job class hasn't been found or isn't derived from %s class."
                    + CustomizableJob.class.getSimpleName());
            return;
        }

        int jobStatus = ToolRunner.run(job, args);
        System.exit(jobStatus);
    }
}
