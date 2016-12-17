/**
    The file is derived from Contrail Project which is developed by Michael Schatz,
    Jeremy Chambers, Avijit Gupta, Rushil Gupta, David Kelley, Jeremy Lewi,
    Deepak Nettem, Dan Sommer, Mihai Pop, Schatz Lab and Cold Spring Harbor Laboratory,
    and is released under Apache License 2.0 at:
    http://sourceforge.net/apps/mediawiki/contrail-bio/

    Modified by davbzh on 2016-12-16.
*/

package brush;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;


public class Stats extends Configured implements Tool {
    private static final Logger sLogger = Logger.getLogger(Stats.class);

    private static final int n50contigthreshold = 100;

    //private static class StatsMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
    public static class StatsMapper extends Mapper<LongWritable, Text, Text, Text>

    {
        private long   smallcnt = 0;
        private long   smallsum = 0;
        private long   smalldeg = 0;
        private double smallcov = 0;

        private long   medcnt = 0;
        private long   medsum = 0;
        private long   meddeg = 0;
        private double medcov = 0;

        //\\//:
        //OutputCollector<Text,Text> mOutput = null;
        Context mOutput = null;

        private static Set<String> fields = new HashSet<String>();

        private static Node node = new Node();

        //\\//:
        //public void configure(JobConf job)
        public void configure(Configuration job)
        {
            fields.add(Node.STR);
            fields.add(Node.COVERAGE);
            fields.add("ff");
            fields.add("fr");
            fields.add("rr");
            fields.add("rf");
        }

        //\\//:
        //public void map(LongWritable lineid, Text nodetxt, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
        public void map(LongWritable lineid, Text nodetxt, Context context) throws IOException, InterruptedException
        {
            //\\//:
            //mOutput = output;
            mOutput = context;

            node.fromNodeMsg(nodetxt.toString(), fields);

            //String str  = node.str();
            //String raw  = node.str_raw();
            //String edges = node.edges();

            int len     = node.len();
            int fdegree = node.degree("f");
            int rdegree = node.degree("r");
            float cov   = node.cov();

            if (len < n50contigthreshold)
            {
                if (len >= 50)
                {
                    medcnt++;
                    medsum += len;
                    meddeg += (fdegree + rdegree) * len;
                    medcov += cov * len;
                    //reporter.incrCounter("Brush", "mednodes", 1);
                    context.getCounter("Brush", "mednodes").increment(1);
                }

                smallcnt++;
                smallsum += len;
                smalldeg += (fdegree + rdegree) * len;
                smallcov += cov * len;

                //reporter.incrCounter("Brush", "smallnodes", 1);
                context.getCounter("Brush", "mednodes").increment(1);

            }
            else
            {
                //\\//:
                //output.collect(new Text(Integer.toString(len)), new Text(Integer.toString(fdegree) + "\t" + Integer.toString(rdegree) + "\t" + Float.toString(cov)));
                context.write(new Text(Integer.toString(len)), new Text(Integer.toString(fdegree) + "\t" + Integer.toString(rdegree) + "\t" + Float.toString(cov)));

            }

            //\\//:
            //reporter.incrCounter("Brush", "nodes", 1);
            context.getCounter("Brush", "nodes").increment(1);

        }

        public void close() throws IOException, InterruptedException
        {
            if (mOutput != null)
            {
                if (smallcnt > 0)
                {
                    //\\//:
                    //mOutput.collect(new Text("SHORT"),new Text("1" + "\t" + Long.toString(smallcnt) + "\t" + Long.toString(smallsum) + "\t" + Long.toString(smalldeg) + "\t" + Double.toString(smallcov)));
                    mOutput.write(new Text("SHORT"),new Text("1" + "\t" + Long.toString(smallcnt) + "\t" + Long.toString(smallsum) + "\t" + Long.toString(smalldeg) + "\t" + Double.toString(smallcov)));

                }

                if (medcnt > 0)
                {
                    //\\//:
                    //mOutput.collect(new Text("SHORT"), new Text("50" + "\t" + Long.toString(medcnt) + "\t" + Long.toString(medsum) + "\t" + Long.toString(meddeg) + "\t" + Double.toString(medcov)));
                    mOutput.write(new Text("SHORT"), new Text("50" + "\t" + Long.toString(medcnt) + "\t" + Long.toString(medsum) + "\t" + Long.toString(meddeg) + "\t" + Double.toString(medcov)));

                }

                smallcnt = 0;
                smallsum = 0;
                smalldeg = 0;
                smallcov = 0;

                medcnt = 0;
                medsum = 0;
                meddeg = 0;
                medcov = 0;
            }
        }
    }

    //\\//:
    //private static class StatsReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>
    public static class StatsReducer extends Reducer<Text, Text, Text, Text>
    {
        //OutputCollector<Text,Text> mOutput = null;
        Context mOutput = null;

        private long N50_TARGET = 0;

        private final int TOPCNT = 10;

        private List<Integer> n50sizes = new ArrayList<Integer>();

        private static int [] cutoffs =
                {        1,      50,    100,    250,    500,
                        1000,    5000,  10000,  15000,  20000,
                        25000,   30000,  35000,  40000,  50000,
                        75000,  100000, 125000, 150000, 200000,
                        250000,  500000, 750000, 1000000 };

        private long   [] cnts  = new long   [cutoffs.length];
        private long   [] sums  = new long   [cutoffs.length];
        private long   [] degs  = new long   [cutoffs.length];
        private long   [] n50s  = new long   [cutoffs.length];
        private long   [] n50is = new long   [cutoffs.length];
        private double [] covs  = new double [cutoffs.length];

        //public void configure(JobConf job) {
        public void configure(Configuration job) {
            N50_TARGET = Long.parseLong(job.get("N50_TARGET"));
        }

        //public void reduce(Text key, Iterator<Text> iter, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
        public void reduce(Text key, Iterable<Text> iter,  Context context) throws IOException, InterruptedException
        {
            mOutput = context;

            if (key.toString().compareTo("SHORT") == 0)
            {
                //    my ($tag, $cutoff, $cnt, $sum, $degree, $cov) = split /\t/, $_;



                //\\//:
                //while (iter.hasNext())
                for(Text valstr : iter)
                {
                    //String valstr = iter.next().toString();
                    //String [] values = valstr.split("\t");
                    String [] values = valstr.toString().split("\t");

                    //System.err.println(key.toString() + " " + valstr + "\n");

                    int cutoff = Integer.parseInt(values[0]);
                    long cnt    = Long.parseLong(values[1]);
                    long sum    = Long.parseLong(values[2]);
                    long deg    = Long.parseLong(values[3]);
                    double cov  = Double.parseDouble(values[4]);

                    int ci = -1;
                    for (int i = 0; i < cutoffs.length; i++)
                    {
                        if (cutoffs[i] == cutoff)
                        {
                            ci = i;
                            break;
                        }
                    }

                    if (ci == -1) { throw new IOException("Couldn't find cutoff index for " + cutoff); }

                    cnts[ci] += cnt;
                    sums[ci] += sum;
                    degs[ci] += deg;
                    covs[ci] += cov;
                }
            }
            else
            {
//				my ($len, $fdegree, $rdegree, $cov) = split /\t/, $_;

                int len = Integer.parseInt(key.toString());

                //\\//:
                //while (iter.hasNext())
                for(Text valstr : iter)
                {
                    //String valstr = iter.next().toString();
                    //String [] values = valstr.split("\t");
                    String [] values = valstr.toString().split("\t");

                    //System.err.println(key.toString() + " " + valstr + "\n");

                    int fdegree = Integer.parseInt(values[0]);
                    int rdegree = Integer.parseInt(values[1]);
                    float cov   = Float.parseFloat(values[2]);

                    if (len >= n50contigthreshold)
                    {
                        n50sizes.add(len);
                    }

                    for (int i = 0; i < cutoffs.length; i++)
                    {
                        if (len >= cutoffs[i])
                        {
                            cnts[i]++;
                            sums[i] += len;
                            degs[i] += (fdegree + rdegree) * len;
                            covs[i] += cov * len;
                        }
                    }
                }
            }
        }

        public void close() throws IOException, InterruptedException
        {
            if (mOutput != null)
            {
                if (cnts[0] == 0) { throw new IOException("No contigs"); }

                Collections.sort(n50sizes); // ascending sort

                //mOutput.collect(new Text(),
                //		new Text(String.format("%-11s% 10s% 10s% 13s% 10s% 10s% 10s% 10s\n",
                //				"Threshold", "Cnt", "Sum", "Mean", "N50", "N50Cnt", "Deg", "Cov")));

                //mOutput.collect(new Text("Cutoff"), new Text("Cnt\tSum\tMean\tN50\tN50Cnt\tDeg\tCov"));
                mOutput.write(new Text("Cutoff"), new Text("Cnt\tSum\tMean\tN50\tN50Cnt\tDeg\tCov"));

                long n50sum = 0;
                int n50candidates = n50sizes.size();

                // find the largest cutoff with at least 1 contig
                int curcutoff = -1;

                for (int i = cutoffs.length - 1; i >= 0; i--)
                {
                    if (cnts[i] > 0)
                    {
                        curcutoff = i;
                        break;
                    }
                }

                // compute the n50 for each cutoff in descending order
                long n50cutoff = sums[curcutoff] / 2;

                for (int i = 0; (i < n50candidates) && (curcutoff >= 0); i++)
                {
                    int val = n50sizes.get(n50candidates - 1 - i);
                    n50sum += val;

                    if (n50sum >= n50cutoff)
                    {
                        n50s[curcutoff]  = val;
                        n50is[curcutoff] = i+1;

                        curcutoff--;

                        while (curcutoff >= 0)
                        {
                            n50cutoff = sums[curcutoff] / 2;

                            if (n50sum >= n50cutoff)
                            {
                                n50s[curcutoff]  = val;
                                n50is[curcutoff] = i+1;

                                curcutoff--;
                            }
                            else
                            {
                                break;
                            }
                        }
                    }
                }

                DecimalFormat df = new DecimalFormat("0.00");

                // print stats at each cutoff
                for (int i = cutoffs.length - 1; i >= 0; i--)
                {
                    int  t      = cutoffs[i];
                    long c      = cnts[i];

                    if (c > 0)
                    {
                        long s      = sums[i];
                        long n50    = n50s[i];
                        long n50cnt = n50is[i];;

                        double degree = (double) degs[i] / (double) s;
                        double cov    = (double) covs[i] / (double) s;

                        //mOutput.collect(new Text(),
                        //		new Text(String.format(">%-10s% 10d% 10d%13.02f%10d%10d%10.02f%10.02f",
                        //				t, c, s, (c > 0 ? s/c : 0.0), n50, n50cnt, degree, cov)));
                        //\\//:
                        //mOutput.collect(new Text(">" + t), new Text(c + "\t" + s + "\t" + df.format(c > 0 ? (float) s/ (float) c : 0.0) + "\t" + 	n50 + "\t" + n50cnt + "\t" + df.format(degree) + "\t" + df.format(cov)));
                        mOutput.write(new Text(">" + t), new Text(c + "\t" + s + "\t" + df.format(c > 0 ? (float) s/ (float) c : 0.0) + "\t" + 	n50 + "\t" + n50cnt + "\t" + df.format(degree) + "\t" + df.format(cov)));

                    }
                }

                // print the top N contig sizes
                if (n50candidates > 0)
                {
                    //\\//:
                    //mOutput.collect(new Text(""), new Text(""));
                    mOutput.write(new Text(""), new Text(""));

                    long topsum = 0;
                    for (int i = 0; (i < TOPCNT) && (i < n50candidates); i++)
                    {
                        int val = n50sizes.get(n50candidates - 1 - i);
                        topsum += val;
                        int j = i+1;

                        //\\//:
                        //mOutput.collect(new Text("max_" + j + ":"), new Text(val + "\t" + topsum));
                        mOutput.write(new Text("max_" + j + ":"), new Text(val + "\t" + topsum));
                    }
                }

                // compute the N50 with respect to user specified genome size
                if (N50_TARGET > 0)
                {
                    //\\//:
                    //mOutput.collect(new Text(""), new Text(""));
                    //mOutput.collect(new Text("global_n50target:"), new Text(Long.toString(N50_TARGET)));
                    mOutput.write(new Text(""), new Text(""));
                    mOutput.write(new Text("global_n50target:"), new Text(Long.toString(N50_TARGET)));

                    n50sum = 0;
                    n50cutoff = N50_TARGET/2;
                    boolean n50found = false;

                    for (int i = 0; i < n50candidates; i++)
                    {
                        int val = n50sizes.get(n50candidates - 1 - i);
                        n50sum += val;

                        if (n50sum >= n50cutoff)
                        {
                            int n50cnt = i + 1;
                            n50found = true;

                            //\\//:
                            //mOutput.collect(new Text("global_n50:"),    new Text(Integer.toString(val)));
                            //mOutput.collect(new Text("global_n50cnt:"), new Text(Integer.toString(n50cnt)));
                            mOutput.write(new Text("global_n50:"),    new Text(Integer.toString(val)));
                            mOutput.write(new Text("global_n50cnt:"), new Text(Integer.toString(n50cnt)));

                            break;
                        }
                    }

                    if (!n50found)
                    {
                        //\\//:
                        //mOutput.collect(new Text("global_n50:"),    new Text("<" + n50contigthreshold));
                        //mOutput.collect(new Text("global_n50cnt:"), new Text(">" + n50candidates));
                        mOutput.write(new Text("global_n50:"),    new Text("<" + n50contigthreshold));
                        mOutput.write(new Text("global_n50cnt:"), new Text(">" + n50candidates));

                    }
                }
            }
        }
    }

    //\\//:
    //public RunningJob run(String inputPath, String outputPath) throws Exception
    public int run(String inputPath, String outputPath) throws Exception
    {
        sLogger.info("Tool name: Stats");
        sLogger.info(" - input: "  + inputPath);
        sLogger.info(" - output: " + outputPath);

        //\\//:
        //JobConf conf = new JobConf(Stats.class);
        //When implementing tool
        Configuration conf = new Configuration();

        // Create job:
        //\\//:
        //conf.setJobName("Stats " + inputPath);
        Job job = Job.getInstance(conf, "Stats " + inputPath);
        job.setJarByClass(Stats.class);

        //\\//: Please look TODO note bellow:
        //TODO: what parameters are was coming from configuration file???
        //BrushConfig.initializeConfiguration(conf);
        //conf.setNumReduceTasks(1);
        job.setNumReduceTasks(1);

        //\\//:
        //FileInputFormat.addInputPath(conf, new Path(inputPath));
        //FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        //\\//:
        //conf.setInputFormat(TextInputFormat.class);
        //conf.setOutputFormat(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //\\//:
        //conf.setMapOutputKeyClass(Text.class);
        //conf.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //\\//:
        //conf.setOutputKeyClass(Text.class);
        //conf.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //\\//:
        //conf.setMapperClass(StatsMapper.class);
        //conf.setReducerClass(StatsReducer.class);
        job.setMapperClass(StatsMapper.class);
        job.setReducerClass(StatsReducer.class);

        //delete the output directory if it exists already
        FileSystem.get(conf).delete(new Path(outputPath), true);

        //\\//:
        //return JobClient.runJob(conf);
        return job.waitForCompletion(true) ? 0 : 1;
    }


    public int run(String[] args) throws Exception
    {
        String inputPath  = ""; //args[0];
        String outputPath = ""; //args[1];

        //\\//: Please look TODO note bellow
        //TODO: N50_TARGET was comming from configuration file???
        //BrushConfig.N50_TARGET = 1234;
        int N50_TARGET = 1234;

        run(inputPath, outputPath);

        return 0;
    }

    public static void main(String[] args) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new Stats(), args);
        System.exit(res);
    }
}
