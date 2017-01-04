/**
 EdgeRemoval.java
 2012 Ⓒ CloudBrush, developed by Chien-Chih Chen (rocky@iis.sinica.edu.tw),
 released under Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
 at: https://github.com/ice91/CloudBrush
 */
/**
 * Modified by davbzh on 2016-12-27.
 */

package brush;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class EdgeRemoval extends Configured implements Tool {
    private static final Logger sLogger = Logger.getLogger(EdgeRemoval.class);

    ///////////////////////////////////////////////////////////////////////////
    // EdgeRemovalMapper
    public static class EdgeRemovalMapper  extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable lineid, Text nodetxt, Context context) throws IOException, InterruptedException {
            Node node = new Node();
            node.fromNodeMsg(nodetxt.toString());
            List<String> r_edges = node.getRemovalEdges();
            if (r_edges != null) {
                for (String r_edge : r_edges) {
                    String[] vals = r_edge.split("\\|");
                    String id = vals[0];
                    String dir = vals[1];
                    String dead = vals[2];
                    int oval = Integer.parseInt(vals[3]);
                    context.write(new Text(id), new Text(Node.KILLLINKMSG + "\t" + dir + "\t" + dead + "\t" + oval));
                    context.getCounter("Brush", "edgesremoved").increment(1);
                }
                node.clearRemovalEdge();
            }
            context.write(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
            context.getCounter("Brush", "nodes").increment(1);
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // EdgeRemovalReducer
    public static class EdgeRemovalReducer extends Reducer<Text, Text, Text, Text> {
        public class RemoveLink {
            public String deaddir;
            public String deadid;
            public int oval_size;
            public RemoveLink(String[] vals, int offset) throws IOException {
                if (!vals[offset].equals(Node.KILLLINKMSG)) {
                    throw new IOException("Unknown msg");
                }
                deaddir = vals[offset + 1];
                deadid = vals[offset + 2];
                oval_size = Integer.parseInt(vals[offset + 3]);
            }
        }

        public void reduce(Text nodeid, Iterable<Text> iter,  Context context) throws IOException, InterruptedException {
            Node node = new Node(nodeid.toString());
            int sawnode = 0;
            boolean killnode = false;
            float extracov = 0;
            List<RemoveLink> links = new ArrayList<RemoveLink>();
            for(Text msg : iter) {
                String [] vals = msg.toString().split("\t"); //\\//: tokenize it
                if (vals[0].equals(Node.NODEMSG)) {
                    node.parseNodeMsg(vals, 0);
                    sawnode++;
                } else if (vals[0].equals(Node.KILLLINKMSG)) {
                    RemoveLink link = new RemoveLink(vals, 0);
                    links.add(link);
                } else {
                    throw new IOException("Unknown msgtype: " + msg);
                }
            }
            if (sawnode != 1) {
                if (node.getEdges("ff") == null && node.getEdges("rr") == null && node.getEdges("fr") == null
                        && node.getEdges("rf") == null) {
                    // do nothing, possible black node
                } else {
                    throw new IOException(
                            "ERROR: Didn't see exactly 1 nodemsg (" + sawnode + ") for " + nodeid.toString());
                }
            }
            if (links.size() > 0) {
                for (RemoveLink link : links) {
                    if (node.hasEdge(link.deaddir, link.deadid, link.oval_size)) {
                        node.removelink(link.deadid, link.deaddir, link.oval_size);
                    }
                    context.getCounter("Brush", "linksremoved").increment(1);
                }

            }
            context.write(nodeid, new Text(node.toNodeMsg()));
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // Run Tool
    public int run(String inputPath, String outputPath) throws Exception {
        sLogger.info("Tool name: EdgeRemoval");
        sLogger.info(" - input: " + inputPath);
        sLogger.info(" - output: " + outputPath);
        //\\//:
        Configuration conf = new Configuration();
        //\\//:
        // Create job:
        Job job = Job.getInstance(conf, "EdgeRemoval " + inputPath);
        job.setJarByClass(EdgeRemoval.class);
        // Setup input and output paths
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        //\\//:
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //\\//:
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // conf.setBoolean("mapred.output.compress", true);
        //\\//:
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // Setup MapReduce job
        job.setMapperClass(EdgeRemovalMapper.class);
        job.setReducerClass(EdgeRemovalReducer.class);
        // delete the output directory if it exists already
        FileSystem.get(conf).delete(new Path(outputPath), true);
        //\\//:
        return job.waitForCompletion(true) ? 0 : 1;
    }

    ///////////////////////////////////////////////////////////////////////////
    // Parse Arguments and run
    public int run(String[] args) throws Exception {
        String inputPath  = args[0];
        String outputPath = args[1];
        run(inputPath, outputPath);
        return 0;
    }
    ///////////////////////////////////////////////////////////////////////////
    // Main
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new EdgeRemoval(), args);
        System.exit(res);
    }
}
