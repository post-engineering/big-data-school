package com.griddynamics.bigdata.util;

import com.griddynamics.bigdata.CustomizableJob;
import org.apache.commons.cli.*;
import org.apache.hadoop.fs.Path;
import org.reflections.Reflections;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * TODO
 */
public class ExtendedOptionsParser {

    private final static CommandLineParser CMD_PARSER = new GnuParser();
    private String[] args;
    private CommandLine cmd;
    private String rootPackageToScan;

    public ExtendedOptionsParser(String rootPackageToScan, String[] args) throws ParseException {
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
            throw new Exception("Please specify job class to run. Ex.: -j MyJob");

        }

        String jobId = cmd.getOptionValue(ExtendedOptionKeys.JOB_ID.getOptionKey());

        Reflections reflections = new Reflections(rootPackageToScan);
        Set<Class> annotated = reflections.getTypesAnnotatedWith(CustomJob.class)
                .stream()
                .filter(c -> c.getSimpleName().equals(jobId))
                .collect(Collectors.toSet());

        if (annotated == null)
            return null;

        Class cls = Class.forName(annotated.iterator().next().getName());
        if (CustomizableJob.class.isAssignableFrom(cls)) {
            job = (CustomizableJob) cls.newInstance();
        } else {
            return null;
        }

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

    /**
     * TODO
     *
     * @param
     * @return
     */
    public String getExtendedOptionsUsage() {
        StringBuilder sb = new StringBuilder().append("Extended options are:\n");
        ExtendedOptionKeys.ALL_OPTIONS.getOptions().
                forEach(option -> sb.append(String.format("%s \t %s \n",
                        ((Option) option).getArgName(),
                        ((Option) option).getDescription())));

        return sb.toString();
    }

    /**
     * TODO
     */
    private enum ExtendedOptionKeys {
        JOB_ID("j", true, "id of a Job to run"),
        INPUT("i", true, "input path", true),
        OUTPUT("o", true, "output pat", true),
        CLEAN_OUTPUT_IF_EXISTS("c", false, "clean output if exists");

        private final static Options ALL_OPTIONS = new Options();

        static {
            for (ExtendedOptionKeys opt : ExtendedOptionKeys.values()) {
                ALL_OPTIONS.addOption(opt.getOption());
            }
        }

        private Option option;

        ExtendedOptionKeys(String optKey, boolean hasArgument, String description, boolean isRequired) {
            option = new Option(optKey, hasArgument, description);
            option.setRequired(isRequired);
        }

        ExtendedOptionKeys(String optKey, boolean hasArgument, String description) {
            this(optKey, hasArgument, description, false);
        }

        public Option getOption() {
            return option;
        }

        public String getOptionKey() {
            return option.getOpt();
        }

    }

}

