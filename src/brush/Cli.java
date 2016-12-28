package brush;

/**
 * Created by davbzh on 2016-12-16.
 */

import org.apache.commons.cli.*;

public class Cli {
    private String[] args = null;
    private Options options = new Options();

    public Cli(String[] args) {
        this.args = args;
        options.addOption("h", "help", false, "show help.");
        options.addOption("i", "input", true, "input file directory location");
        options.addOption("o", "output", false, "output file directory location");
    }

    public void parse() throws Exception {
        //DefaultParser() is in cli-1.3.1
        //CommandLineParser parser = new DefaultParser();
        CommandLineParser parser = new GnuParser();

        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);

            if (cmd.hasOption("h")) {
                help();
            }

            if (cmd.hasOption("i") ) {
                //Mark pair end reads
                SetPairEndReads sp = new SetPairEndReads();
                sp.run(cmd.getOptionValue("i"),"/test_out1");

                //chop k-mers and determine read relationship by k-mer overlapping positions
                MatchPrefix mp = new MatchPrefix();
                mp.run("/test_out1","/test_out2", "21", "99");

                //Verify and group edges
                VerifyOverlap vo = new VerifyOverlap();
                vo.run("/test_out2", "/test_out3");

                //TransitiveReduction
                TransitiveReduction tr = new TransitiveReduction();
                tr.run("/test_out3","/test_out4", "0.00f", "99");

                //EdgeRemoval
                EdgeRemoval er = new EdgeRemoval();
                er.run("/test_out4","/test_out5");

                //Compressible
                Compressible cmp = new Compressible();
                cmp.run("/test_out5","/test_out6");

            } else {
                help();
            }

        } catch (ParseException e) {
            help();
        }
    }

    private void help() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("brush assembler", options);
        System.exit(0);
    }



}
