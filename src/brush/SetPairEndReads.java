package brush;

/**
 * Created by davbzh on 2016-12-16.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;

//\\//: why we import this?
import brush.MatchPrefix;
import brush.Stats;

public class SetPairEndReads extends Configured implements Tool {
    //\\//:
    //TODO: Check what MatchPrefix.class is doing here
    private static final Logger sLogger = Logger.getLogger(MatchPrefix.class);

    public static class SetPairEndReadsMapper extends  Mapper<LongWritable, Text, Text, Text>  {
        //TODO: I need global config file, or direct input to define K
        public static int K = 21;

        public void configure(Configuration job) {
            //TODO: this doesn't work
            K = Integer.parseInt(job.get("K"));
        }

        public void map(LongWritable lineid, Text nodetxt, Context context) throws IOException, InterruptedException {
            //\\//: taking interleaved fastq 8 line at a time and tokenize them by '\n'.
            String[] fields = nodetxt.toString().split("\n");

            //for single read fastq files it is 4, for pair-end it is 8
            if (fields.length != 8) {
                context.getCounter("Brush", "input_lines_invalid").increment(1);
                return;
            }

            //\\//: tag here is id of sequence read from illumina run and then try to correct illumina generated read ids, in this case tag
            String tag = fields[0].replaceAll(" ", "_").replaceAll(":", "_").replaceAll("#", "_").replaceAll("-", "_").replaceAll("@", "").replaceAll("/1", "");

            //\\//: convert all lower case dna characters to uppercase
            String seq_f = fields[1].toUpperCase();
            String seq_r = fields[5].toUpperCase();

            //\\//:
            // trim Ns off the at the end of read, if there is any
            seq_f = Node.trimNs(seq_f);
            seq_r = Node.trimNs(seq_r);

            // \\//: Check for non-dna characters and if there is any return number
            // of them
            if (seq_f.matches(".*[^ACGT].*")) {
                context.getCounter("Brush", "f_reads_with_non_dna_characters").increment(1);
            }
            if (seq_r.matches(".*[^ACGT].*")) {
                context.getCounter("Brush", "r_reads_with_non_dna_characters").increment(1);
            }

            //\\//: check for short reads, should not be less than a indicated kmer length
            if (seq_f.length() <= K || seq_r.length() <= K){
                context.getCounter("Brush", "very_reads_short").increment(1);
                return;
            }

            // Now emit the prefix of the reads
            Node node = new Node(tag);

            String seq = seq_f + "!" + seq_r;
            node.setpairstr(seq_f,seq_r);
            node.setCoverage(2);
            System.out.println("Mapper creating new node : " + node.getNodeId() + "\t" + node.toNodeMsg() + ";");

            //----------------------------------------------------------------------------------------------
            //TODO: This is not necessary
            String prefix = Node.str2dna(seq_f.substring(0, K));
            //----------------------------------------------------------------------------------------------

            //\\//: sending to output collector reverse nodes which in turn passes the same to reducer
            context.write(new Text(prefix), new Text("r" + "\t" + node.toNodeMsg(true)));
            context.getCounter("Brush", "reads_good").increment(1);
        }
    }

    //\\//:
    public static class SetPairEndReadsReducer extends Reducer<Text, Text, Text, Text> {
        private static int K = 0;

        //TODO:
        public void configure(Configuration job) {
            K = Integer.parseInt(job.get("K"));
        }

        public void reduce(Text prefix, Iterable<Text> iter,  Context context) throws IOException, InterruptedException {
            List<Node> nodes = new ArrayList<Node>();

            Map<String, Node> nodes_f = new HashMap<String, Node>();

            for(Text msg : iter) {
                String [] vals = msg.toString().split("\t");

                //\\//: Cheking the "N" string from mapper
                if (vals[2].equals(Node.NODEMSG))   {
                    //\\//: Generate new node object with id
                    Node node = new Node(vals[1]);

                    //\\//: @see parseNodeMsg() in Node.java
                    node.parseNodeMsg(vals, 2);
                    //Node after_node = node;

                    //\\//: add to nodes
                    nodes_f.put(node.getNodeId() + "|" + vals[0], node);
                    nodes.add(node);
                    System.out.println("nodes_toString: " + nodes.size());

                } else {
                    throw new IOException("Unknown msgtype: " + msg);
                }
            }

            System.out.println("nodes_toString: " + nodes.size());


            for (int i = 0; i < nodes.size(); i++) { //\\//: loop over forward nodes
                Node node = nodes.get(i);
                System.out.println("node from nodes" + node.getNodeId());

                //\\//:
                context.write(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
                context.getCounter("Brush", "nodecount").increment(1);
            }
        }
    }

    //\\//:
    public int run(String inputPath, String outputPath) throws Exception {

        //\\//:
        //TODO: add k-mer as input
        sLogger.info("Tool name: SetPairEndReads");
        sLogger.info(" - input: "  + inputPath);
        sLogger.info(" - output: " + outputPath);
        int K = 21;

        //\\//:
        //When implementing tool
        Configuration conf = new Configuration();

        //\\//: May be useful:
        conf.set("mapred.job.tracker", "192.168.56.111:9001");
        conf.set("fs.defaultFS", "hdfs://192.168.56.111:9000");
        //on the shell:
        //hadoop jar SetPairEndReads.jar brush.SetPairEndReads -fs hdfs://192.168.56.111:9000 -jt 192.168.56.111:9001

        //\\//:
        //BrushConfig.initializeConfiguration(conf);


        // Create job:
        Job job = Job.getInstance(conf, "SetPairEndReads " + inputPath + " " + K);
        job.setJarByClass(SetPairEndReads.class);

        // Setup input and output paths
        FileInputFormat.addInputPath(job, new Path(inputPath)); //args[0]
        FileOutputFormat.setOutputPath(job, new Path(outputPath)); //args[1]

        //\\//:
        job.setInputFormatClass(InterleavedFastqInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //\\//:
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Specify the type of output keys and values
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //\\//:TODO:
        //conf.setBoolean("mapred.output.compress", true);
        //conf.setClass("mapred.output.compression.codec", GzipCodec.class,CompressionCodec.class);

        //\\//:Setup MapReduce job
        job.setMapperClass(SetPairEndReadsMapper.class);
        job.setReducerClass(SetPairEndReadsReducer.class);

        //delete the output directory if it exists already
        FileSystem.get(conf).delete(new Path(outputPath), true); //args[1]

        //\\//:
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public int run(String[] args) throws Exception {
        //TODO: add k-mer as input
        String inputPath  = args[0];
        String outputPath = args[1];
        int K = 21;

        long starttime = System.currentTimeMillis();

        run(inputPath, outputPath);

        long endtime = System.currentTimeMillis();

        float diff = (float) (((float) (endtime - starttime)) / 1000.0);

        System.out.println("Runtime: " + diff + " s");

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SetPairEndReads(), args);
        System.exit(res);
    }
}
