/**
 * Created by davbzh on 2017-01-08.
 */

package brush;

import java.io.IOException;
import java.util.Collections;
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

public class DigraphMatix extends Configured implements Tool{
    private static final Logger sLogger = Logger.getLogger(DigraphMatix.class);

    public static class DigraphMatixMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable lineid, Text nodetxt, Context context) throws IOException, InterruptedException {
            Node node = new Node();
            node.fromNodeMsg(nodetxt.toString());

            //emit:
            context.write(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
            context.getCounter("Brush", "nodes").increment(1);
        }
    }

    public static class DigraphMatixReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text nodeid, Iterable<Text> iter, Context context) throws IOException, InterruptedException {
            Node node = new Node(nodeid.toString());
            for (Text msg : iter) {
                String[] vals = msg.toString().split("\t");
                System.out.println("DigraphMatixReducer "+vals[0]+"\t"+vals[1]+"\t"+vals[2]+"\t"+vals[4]+"\tlength:\t"+vals.length);
                if (vals[0].equals(Node.NODEMSG)) {
                    node.parseNodeMsg(vals, 0);

                    for (String key : Node.edgetypes) {
                        List<String> edges = node.getEdges(key);
                        if (edges != null) {
                            // emited node
                            context.getCounter("Has edge", "nodes").increment(1);
                            for (int i = 0; i < edges.size(); i++) {
                                String[] ed_vals = edges.get(i).split("!");
                                String edge_id = ed_vals[0];
                                String oval_size = ed_vals[1];
                                String con = key;
                                //emit:
                                if (key == "ff" || key == "fe" || key == "fr" || key == "ef"){
                                    context.write(new Text(node.getNodeId()), new Text(edge_id));
                                } else if (key == "rr" || key == "re" || key == "rf" || key ==  "er"){
                                    context.write(new Text(edge_id), new Text(node.getNodeId()));
                                }
                            }
                        } else {
                            context.getCounter("NULL edge", "nodes").increment(1);
                        }
                    }
                    context.getCounter("NODEMSG", "nodecount").increment(1);
                } else {
                    throw new IOException("Unknown msgtype: " + msg);
                }

            }

        }
    }

    public int run(String inputPath, String outputPath) throws Exception {
        sLogger.info("Tool name: DigraphMatix");
        sLogger.info(" - input: " + inputPath);
        sLogger.info(" - output: " + outputPath);
        //\\//:
        Configuration conf = new Configuration();
        //\\//:
        // Create job:
        Job job = Job.getInstance(conf, "DigraphMatix " + inputPath);
        job.setJarByClass(DigraphMatix.class);
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
        job.setMapperClass(DigraphMatixMapper.class);
        job.setReducerClass(DigraphMatixReducer.class);
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
        int res = ToolRunner.run(new Configuration(), new DigraphMatix(), args);
        System.exit(res);
    }

}
