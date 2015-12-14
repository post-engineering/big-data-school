package com.griddynamics.bigdata.util;


import com.griddynamics.bigdata.CustomizableJob;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NoSuchElementException;

/**
 * TODO
 */
public class JobRunner {
    private final static Logger LOG = LoggerFactory.getLogger(JobRunner.class);

    public static void main(String[] args) {
        try {
            CustomizableJob job = CustomizableJob.parseJob(args);

            if (job == null) {
                throw new NoSuchElementException();
            }

            int jobStatus = ToolRunner.run(job, args);
            System.exit(jobStatus);

        } catch (NoSuchElementException e) {
            LOG.error(String.format("Specified custom job class hasn't been found or isn't derived from %s class."
                    , CustomizableJob.class.getSimpleName()));
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }

    }
}
