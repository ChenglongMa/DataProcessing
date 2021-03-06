package utils;

import org.apache.commons.cli.*;

//import org.apache.commons.cli.*;

/**
 * @author Chenglong Ma
 */
public class Command {
    private final CommandLine cmd;

    public Command(String[] args) {
        cmd = buildCli(args);
    }

    /**
     * The usage of command line options
     *
     * @param options
     */
    private void help(Options options) {
        String header = "An application for data processing\n\n";
        String footer = "\nHave fun:)";
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("DataProcessing", header, options, footer, true);
        System.exit(-1);
    }

    /**
     * Build options based on requirements
     *
     * @return
     */
    private Options buildOptions() {
        //build options
        Options options = new Options();

        Option.Builder datasetOpt = Option.builder(Opt.TRAIN_PATH)
                .longOpt("train_src")
                .hasArgs()
                .desc("The source file path\n---");
        options.addOption(datasetOpt.build());

        options.addOption(Opt.TEST_PATH, "test", true, "The test set file path\n---");
        options.addOption(Opt.RESULT_PATH, "result", true, "The result file path\n---");
        options.addOption(Opt.SEP, true, "The sep of file");

        options.addOption(Opt.HEADER, "header", true, "The number of headers");
        options.addOption(Opt.ITEM_BASED, "item_based", false, "User based or Item based");
        options.addOption(Opt.SIM_ONLY, "sim_only", false, "Generate sim mat only");

        return options;
    }

    /**
     * Build Command Line Interface
     *
     * @param args
     * @return the properties read from cli
     */
    public CommandLine buildCli(String[] args) {
        System.out.println("reading command line options");
        Options options = buildOptions();
        CommandLineParser parser = new DefaultParser();
        // verify the input commands
        try {
            return parser.parse(options, args);
        } catch (ParseException e) {
            e.printStackTrace();
            // print usage and exit
            help(options);
            return null;
        }
    }

    public String getTrainFile() {
        return getOption(Opt.TRAIN_PATH, null);
    }

    public String getTestFile() {
        return getOption(Opt.TEST_PATH, null);
    }

    public String getResultFile() {
        return getOption(Opt.RESULT_PATH, "res.csv");
    }

    private String getOption(String option, String defValue) {
        return cmd.hasOption(option) ? cmd.getOptionValue(option) : defValue;
    }

    public String getSeparator() {
        return getOption(Opt.SEP, "[ \t,]+");
    }

    public int getNumOfHeaders() {
        return Integer.parseInt(getOption(Opt.HEADER, "1"));
    }

    public boolean isUserBased() {
        return !cmd.hasOption(Opt.ITEM_BASED);
    }

    public boolean isSimOnly() {
        return cmd.hasOption(Opt.SIM_ONLY);
    }

    /**
     * The abbr options used for cli
     */
    private static class Opt {
        //        static final String RECOMMENDER = "r";
        static final String HEADER = "header";
        static final String TRAIN_PATH = "train";
        static final String TEST_PATH = "test";
        static final String RESULT_PATH = "res";
        static final String SEP = "sep";
        static final String ITEM_BASED = "item";
        static final String SIM_ONLY = "sim";
    }
}
