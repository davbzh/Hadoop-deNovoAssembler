/**
 VerifyOverlap.java
 2012 Ⓒ CloudBrush, developed by Chien-Chih Chen (rocky@iis.sinica.edu.tw),
 released under Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
 at: https://github.com/ice91/CloudBrush
 */
/**
 * Modified by davbzh on 2016-12-16.
 */

package brush;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import brush.Node.*;

public class VerifyOverlap extends Configured implements Tool {
    private static final Logger sLogger = Logger.getLogger(VerifyOverlap.class);
    // VerifyOverlapMapper
    public static class VerifyOverlapMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable lineid, Text nodetxt, Context context) throws IOException, InterruptedException {
            Node node = new Node();
            node.fromNodeMsg(nodetxt.toString());
            for (String key : Node.edgetypes) {
                List<String> edges = node.getEdges(key);
                // \\//: check if incoming node from MatchPrefix.java has edges
                if (edges != null) {
                    // \\//: check if so basically add Node.OVALMSG to the
                    // emited node
                    context.getCounter("Has edge", "nodes").increment(1);
                    for (int i = 0; i < edges.size(); i++) {
                        String[] vals = edges.get(i).split("!");
                        String edge_id = vals[0];
                        String oval_size = vals[1];
                        String con = key;
                        //emit:
                        context.write(new Text(edge_id), new Text(Node.OVALMSG + "\t" + node.getNodeId() + "\t" +
                                node.str_raw() + "\t" + con + "\t" + oval_size));
                    }
                } else {
                    context.getCounter("NULL edge", "nodes").increment(1);
                }
            }
            //emit:
            context.write(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
            context.getCounter("Brush", "nodes").increment(1);
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // VerifyOverlapReducer
    public static class VerifyOverlapReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text nodeid, Iterable<Text> iter, Context context) throws IOException, InterruptedException {
            Node node = new Node(nodeid.toString());
            List<OverlapInfo> olist = new ArrayList<OverlapInfo>();
            int sawnode = 0;
            for (Text msg : iter) {
                String[] vals = msg.toString().split("\t");
                if (vals[0].equals(Node.NODEMSG)) {
                    node.parseNodeMsg(vals, 0);
                    sawnode++;
                    context.getCounter("NODEMSG", "nodecount").increment(1);
                } else if (vals[0].equals(Node.OVALMSG)) {
                    OverlapInfo oi = new OverlapInfo(vals, 0);
                    olist.add(oi);
                    context.getCounter("OVALMSG", "nodecount").increment(1);
                } else {
                    throw new IOException("Unknown msgtype: " + msg);
                }
            }
            //\\//: store confirmed edges
            Map<String, List<String>> edges_list = new HashMap<String, List<String>>();
            Map<String, List<String>> IDs_list = new HashMap<String, List<String>>();
            int choices = olist.size();
            if (choices > 0) {
                // Sort overlap strings in order of decreasing overlap size
                Collections.sort(olist, new OvelapSizeComparator());
                // See if there are any pairwise compatible strings
                for (int i = 0; i < choices; i++) {
                    String oval_id = olist.get(i).id;
                    String oval_type = olist.get(i).edge_type;
                    int oval_size = olist.get(i).overlap_size;
                    //String[] reads = olist.get(i).str.split("!");
                    //String read_f = Node.dna2str(reads[0]);
                    //String read_r = Node.dna2str(reads[1]);
                    //Store confirmed edge
                    String edge_content = oval_id + "!" + oval_size;
                    if (edges_list.containsKey(oval_type)) {
                        edges_list.get(oval_type).add(edge_content);
                        IDs_list.get(oval_type).add(oval_id);
                    } else {
                        List<String> tmp_edges = null;
                        tmp_edges = new ArrayList<String>();
                        tmp_edges.add(edge_content);
                        List<String> tmp_IDs = new ArrayList<String>();
                        edges_list.put(oval_type, tmp_edges);
                        tmp_IDs.add(olist.get(i).id);
                        IDs_list.put(oval_type, tmp_IDs);
                    }
                }
            }
            // \\\\\\\\\\\\\\\\\ set Edges
            for (String con : Node.edgetypes) {
                node.clearEdges(con);
                List<String> edges = edges_list.get(con);
                if (edges != null) {
                    node.setEdges(con, edges);
                }
            }
            context.write(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
        }
    }

    public int run(String inputPath, String outputPath) throws Exception {
        sLogger.info("Tool name: VerifyOverlap");
        sLogger.info(" - input: " + inputPath);
        sLogger.info(" - output: " + outputPath);
        //\\//:
        Configuration conf = new Configuration();
        //\\//:
        // Create job:
        Job job = Job.getInstance(conf, "VerifyOverlap " + inputPath);
        job.setJarByClass(VerifyOverlap.class);
        //\\//:
        // Setup input and output paths
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        //\\//:
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //\\//:
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //\\//:
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //\\//:
        // Setup MapReduce job
        job.setMapperClass(VerifyOverlapMapper.class);
        job.setReducerClass(VerifyOverlapReducer.class);
        // delete the output directory if it exists already
        FileSystem.get(conf).delete(new Path(outputPath), true);
        //\\//:
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public int run(String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];
        run(inputPath, outputPath);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new VerifyOverlap(), args);
        System.exit(res);
    }
}