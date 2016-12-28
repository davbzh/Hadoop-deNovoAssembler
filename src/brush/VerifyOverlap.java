package brush;

/**
 * Created by davbzh on 2016-12-16.
 */

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

        public class OverlapInfo {
            public String id;
            public String str;
            public String edge_type;
            public int overlap_size;

            public OverlapInfo(String[] vals, int offset) throws IOException {

                if (!vals[offset].equals(Node.OVALMSG)) {
                    throw new IOException("Unknown message type");
                }
                id = vals[offset + 1];
                str = vals[offset + 2];
                edge_type = vals[offset + 3];
                overlap_size = Integer.parseInt(vals[offset + 4]);
            }
            public String toString() {
                return edge_type + " " + id + " " + overlap_size + " " + str;
            }
        }

        // \\// @see http://stackoverflow.com/questions/8327514/comparison-method-violates-its-general-contract/8327575#8327575
        class OvelapSizeComparator implements Comparator {
            public int compare(Object element1, Object element2) {
                OverlapInfo obj1 = (OverlapInfo) element1;
                OverlapInfo obj2 = (OverlapInfo) element2;
                if ((int) (obj1.overlap_size - obj2.overlap_size) > 0) {
                    return -1;
                } else if ((int) (obj1.overlap_size - obj2.overlap_size) < 0) {
                    return 1;
                } else {
                    return 0;
                }
            }
        }

        // \\//:
        public void reduce(Text nodeid, Iterable<Text> iter, Context context) throws IOException, InterruptedException {
            Node node = new Node(nodeid.toString());
            List<OverlapInfo> olist = new ArrayList<OverlapInfo>();
            int sawnode = 0;
            // \\//:
            for (Text msg : iter) {
                String[] vals = msg.toString().split("\t");
                if (vals[0].equals(Node.NODEMSG)) {
                    node.parseNodeMsg(vals, 0);
                    sawnode++;
                    context.getCounter("NODEMSG", "nodecount").increment(1);
                } else if (vals[0].equals(Node.OVALMSG)) {
                    OverlapInfo oi = new OverlapInfo(vals, 0);
                    System.out.println("oi.str: " + oi.str);
                    olist.add(oi);
                    context.getCounter("OVALMSG", "nodecount").increment(1);
                } else {
                    throw new IOException("Unknown msgtype: " + msg);
                }
            }

            //--------------------------------------------------------------------------------------------------------------------
            // \\ two brush
            if (sawnode != 1 && sawnode != 2) {
                // throw new IOException("ERROR: Didn't see exactly 1 && 2 nodemsg (" + sawnode + ") for " + nodeid.toString());
                System.out.println("ERROR: Didn't see exactly 1 && 2 nodemsg (" + sawnode + ") for " + nodeid.toString());
            }
            //--------------------------------------------------------------------------------------------------------------------

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

                    // \\\\\\\\\\\\\ Store confirmed edge
                    String edge_content = oval_id + "!" + oval_size;
                    if (edges_list.containsKey(oval_type)) {
                        edges_list.get(oval_type).add(edge_content);
                        IDs_list.get(oval_type).add(oval_id);

                        System.out.println("ZZZ=>edges_list: " + edges_list.toString());

                    } else {
                        List<String> tmp_edges = null;
                        tmp_edges = new ArrayList<String>();
                        tmp_edges.add(edge_content);

                        System.out.println( "*****: " + tmp_edges + " " + edges_list.containsValue(tmp_edges) );

                        List<String> tmp_IDs = new ArrayList<String>();
                        //Add only ff or rr edges if there are identical fr or rf edges
                        if ( (oval_type.equals("ff") || oval_type.equals("rr"))) {
                            edges_list.put(oval_type, tmp_edges);
                            tmp_IDs.add(olist.get(i).id);
                            IDs_list.put(oval_type, tmp_IDs);
                        } else if ( (oval_type.equals("rf") || oval_type.equals("fr"))) {
                            if ( edges_list.containsKey("ff") && edges_list.get("ff").equals(tmp_edges) ) {
                                System.out.println("ff_tmp_edges: " + tmp_edges );
                            }
                            else  if ( edges_list.containsKey("rr") && edges_list.get("rr").equals(tmp_edges) ){
                                System.out.println("rr_tmp_edges: " + tmp_edges );
                            }
                            else {
                                edges_list.put(oval_type, tmp_edges);
                                tmp_IDs.add(olist.get(i).id);
                                IDs_list.put(oval_type, tmp_IDs);
                            }
                        }
                        //now check it if there are ff or rr edges with identical positions on fr or rf edges, and remove the latter
                        if ( (oval_type.equals("ff") || oval_type.equals("rr"))) {
                            if ( edges_list.containsKey("fr") && edges_list.get("fr").equals(tmp_edges) ) {
                                edges_list.remove("fr");
                                IDs_list.remove("fr");
                            }
                            if ( edges_list.containsKey("rf") && edges_list.get("rf").equals(tmp_edges)  ){
                                edges_list.remove("rf");
                                IDs_list.remove("rf");
                            }
                        }
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
    // \\//:
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