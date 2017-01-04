/**
 * Created by davbzh on 2017-01-03.
 */

package brush;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class DuplicateEdgeRomoval extends Configured implements Tool {
    private static final Logger sLogger = Logger.getLogger(DuplicateEdgeRomoval.class);
    ///////////////////////////////////////////////////////////////////////////
    // DuplicateEdgeRomovalMapper
    public static class DuplicateEdgeRomovalMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable lineid, Text nodetxt, Context context) throws IOException, InterruptedException {
            Node node = new Node();
            node.fromNodeMsg(nodetxt.toString());
            //emit:
            context.write(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
            context.getCounter("Brush", "nodes").increment(1);
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // DuplicateEdgeRomovalReducer
    public static class DuplicateEdgeRomovalReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text nodeid, Iterable<Text> iter, Context context) throws IOException, InterruptedException {
            Map<String, List<String>> edges_list = new HashMap<String, List<String>>();
            Node node = new Node(nodeid.toString());
            for (Text msg : iter) {
                String[] vals = msg.toString().split("\t");
                if (vals[0].equals(Node.NODEMSG)) {
                    if (vals.length > 5){
                        String key = vals[5].substring(1,3);
                        String[] tmp_edge_content = Arrays.copyOfRange(vals, 6, vals.length);
                        Arrays.sort(tmp_edge_content);
                        String edge_content = Arrays.toString(tmp_edge_content)
                                .replace("[","").replace("]","").replace(", ","\t");
                        if (edges_list.containsKey(key)) {
                            edges_list.get(key).add(edge_content);
                        } else{
                            List<String> tmp_edges = null;
                            tmp_edges = new ArrayList<String>();
                            tmp_edges.add(edge_content);
                            edges_list.put(key, tmp_edges);
                        }
                    } else {
                    }
                    node.parseNodeMsg(vals, 0);
                }
            }

            //-----------------------------------------------------------------
            //now check it if there are ff or rr edges with identical positions on fr or rf edges, and remove the latter
            if ( edges_list.containsKey("ff") && edges_list.containsKey("fr") ) {
                if ( edges_list.get("fr").equals(edges_list.get("ff"))) {
                    edges_list.remove("fr");
                }
            }
            if (edges_list.containsKey("rr") && edges_list.containsKey("rf")){
                if (edges_list.get("rf").equals(edges_list.get("rr"))){
                    edges_list.remove("rf");
                }
            }
            if (edges_list.containsKey("fe") && edges_list.containsKey("ef")){
                if (edges_list.get("ef").equals(edges_list.get("fe"))){
                    edges_list.remove("ef");
                }
            }
            if (edges_list.containsKey("re") && edges_list.containsKey("er")){
                if (edges_list.get("er").equals(edges_list.get("re"))){
                    edges_list.remove("er");
                }
            }
            //Set selected edges and emit
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
        sLogger.info("Tool name: DuplicateEdgeRomoval");
        sLogger.info(" - input: " + inputPath);
        sLogger.info(" - output: " + outputPath);

        Configuration conf = new Configuration();
        // Create job:
        Job job = Job.getInstance(conf, "DuplicateEdgeRomoval " + inputPath);
        job.setJarByClass(DuplicateEdgeRomoval.class);
        // Setup input and output
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // Setup MapReduce job
        job.setMapperClass(DuplicateEdgeRomovalMapper.class);
        job.setReducerClass(DuplicateEdgeRomovalReducer.class);
        // delete the output directory if it exists already
        FileSystem.get(conf).delete(new Path(outputPath), true);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public int run(String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];
        run(inputPath, outputPath);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DuplicateEdgeRomoval(), args);
        System.exit(res);
    }
}
