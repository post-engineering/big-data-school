package com.griddynamics.bigdata.framework;

import com.griddynamics.bigdata.CustomizableJob;
import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;
import org.apache.commons.cli.*;
import org.apache.hadoop.fs.Path;

import java.io.PrintStream;
import java.util.List;
import java.util.stream.Collectors;

/**
 * TODO
 */
public class ExtendedOptionsParser {


    private final static CommandLineParser CMD_PARSER = new DefaultParser();
    private String[] args;
    private CommandLine cmd;
    private String rootPackageToScan;

    public ExtendedOptionsParser(String rootPackageToScan, String[] args) throws Exception {
        this.args = args;
        cmd = CMD_PARSER.parse(ExtendedOptionKeys.ALL_OPTIONS, this.args);
        this.rootPackageToScan = rootPackageToScan;
    }

    /**
     * TODO
     *
     * @return
     * @throws Exception
     */
    public CustomizableJob parseJob() throws Exception {
        CustomizableJob job = null;

        if (!cmd.hasOption(ExtendedOptionsParser.ExtendedOptionKeys.JOB_ID.getOptionKey())) {
            System.err.printf("Please specify job class to run. Ex.: -j MyJob");
            return null;
        }

        String jobId = cmd.getOptionValue(ExtendedOptionKeys.JOB_ID.getOptionKey());

        FastClasspathScanner uberScanner = new FastClasspathScanner(rootPackageToScan);
        List<String> customJobClassNames = uberScanner.scan()
                .getNamesOfClassesWithAnnotation(CustomJob.class)
                .stream()
                .filter(name -> name.endsWith(jobId)).collect(Collectors.toList());


        if (customJobClassNames == null) {
            return null;
        }

        String jobClassName = null;
        if (customJobClassNames.size() > 1) {  //found ambiguous jobs names
            //TODO analyze annotation
        } else {
            jobClassName = customJobClassNames.get(0);
        }

        if (jobClassName == null) {
            return null;
        }
        Class cls = Class.forName(jobClassName);
        if (CustomizableJob.class.isAssignableFrom(cls)) {
            job = (CustomizableJob) cls.newInstance();
        } else {
            return null;
        }

        if (job != null) job.setOptionsParser(this);

        return job;
    }

    /**
     * TODO
     *
     * @return
     * @throws Exception
     */
    public Path getInputPath() throws Exception {
        return getPath(ExtendedOptionKeys.INPUT.getOptionKey());
    }

    /**
     * TODO
     *
     * @return
     * @throws Exception
     */
    public Path getOutputPath() throws Exception {
        return getPath(ExtendedOptionKeys.OUTPUT.getOptionKey());
    }

    private Path getPath(String pathKey) throws Exception {
        String inputFilePath = cmd.getOptionValue(pathKey);

        if (inputFilePath == null) {
            //TODO
            throw new Exception("");
        }
        return new Path(inputFilePath);
    }

    public boolean getCleanOutput() {
        return cmd.hasOption(ExtendedOptionKeys.CLEAN_OUTPUT_IF_EXISTS.getOptionKey());
    }

    public boolean areOptionsValid() {
        return cmd.hasOption(ExtendedOptionKeys.JOB_ID.getOptionKey())
                && cmd.hasOption(ExtendedOptionKeys.INPUT.getOptionKey())
                && cmd.hasOption(ExtendedOptionKeys.OUTPUT.getOptionKey());
    }

    /**
     * TODO
     *
     * @param out
     * @return
     */
    public void printExtendedOptionsUsage(PrintStream out) {
        out.println("Extended options are:");
        ExtendedOptionKeys.ALL_OPTIONS.getOptions().
                forEach(option -> out.printf("%s \t %s", option.getArgName(), option.getDescription()));
    }

    /**
     * TODO
     */
    private enum ExtendedOptionKeys {
        JOB_ID("j", true, "id of a Job to run"),
        INPUT("i", true, "input path"),
        OUTPUT("o", true, "output path"),
        CLEAN_OUTPUT_IF_EXISTS("c", false, "clean output if exists");

        private final static Options ALL_OPTIONS = new Options();

        static {
            for (ExtendedOptionKeys opt : ExtendedOptionKeys.values()) {
                ALL_OPTIONS.addOption(opt.getOption());
            }
        }

        private Option option;

        ExtendedOptionKeys(String optKey, boolean hasArgument, String description) {
            option = new Option(optKey, hasArgument, description);
        }

        public Option getOption() {
            return option;
        }

        public String getOptionKey() {
            return option.getOpt();
        }

    }

}

