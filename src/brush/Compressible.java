/*
Compressible.java
2012 Ⓒ CloudBrush, developed by Chien-Chih Chen (rocky@iis.sinica.edu.tw),
released under Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
at: https://github.com/ice91/CloudBrush

The file is derived from Contrail Project which is developed by Michael Schatz,
Jeremy Chambers, Avijit Gupta, Rushil Gupta, David Kelley, Jeremy Lewi,
Deepak Nettem, Dan Sommer, Mihai Pop, Schatz Lab and Cold Spring Harbor Laboratory,
and is released under Apache License 2.0 at:
http://sourceforge.net/apps/mediawiki/contrail-bio/
*/
/**
 * Modified by davbzh on 2016-12-27.
 */

package brush;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class Compressible extends Configured implements Tool {
    private static final Logger sLogger = Logger.getLogger(Compressible.class);

    public static class CompressibleMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable lineid, Text nodetxt, Context context) throws IOException, InterruptedException {
            Node node = new Node();
            node.fromNodeMsg(nodetxt.toString());

            for (String adj : Node.dirs) {
                node.setCanCompress(adj, false);

                TailInfo next = node.gettail(adj);

                if (next != null /* && node.getBlackEdges() == null */) {
                    if (next.id.equals(node.getNodeId())) {
                        continue;
                    }

                    context.getCounter("Brush", "remotemark").increment(1);
                    context.write(new Text(next.id), new Text(Node.HASUNIQUEP + "\t" + node.getNodeId() + "\t" + adj));
                }
            }

            context.getCounter("Brush", "nodes").increment(1);
            context.write(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
        }
    }

    public static class CompressibleReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> iter,  Context context) throws IOException, InterruptedException {
            Node node = new Node(key.toString());
            Set<String> f_unique = new HashSet<String>();
            Set<String> r_unique = new HashSet<String>();

            int sawnode = 0;

            for(Text msg : iter) {
                String [] vals = msg.toString().split("\t"); //\\//: tokenize it

                if (vals[0].equals(Node.NODEMSG)) {
                    node.parseNodeMsg(vals, 0);
                    sawnode++;
                } else if (vals[0].equals(Node.HASUNIQUEP)) {
                    if (vals[2].equals("f")) {
                        f_unique.add(vals[1]);
                    } else if (vals[2].equals("r")) {
                        r_unique.add(vals[1]);
                    }
                } else {
                    throw new IOException("Unknown msgtype: " + msg);
                }
            }

            if (sawnode != 1) {
                throw new IOException("ERROR: Didn't see exactly 1 nodemsg (" + sawnode + ") for " + key.toString());
            }

            for (String adj : Node.dirs) {
                TailInfo next = node.gettail(adj);

                if (next != null) {
                    if ((next.dir.equals("f") && r_unique.contains(next.id))
                            || (next.dir.equals("r") && f_unique.contains(next.id))) {
                        // for path compress
                        if (node.getBlackEdges() == null) {
                            node.setCanCompress(adj, true);
                        }
                        context.getCounter("Brush", "compressible").increment(1);
                    }
                }
            }
            context.write(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
        }
    }

    public int run(String inputPath, String outputPath) throws Exception {
        sLogger.info("Tool name: Compressible");
        sLogger.info(" - input: " + inputPath);
        sLogger.info(" - output: " + outputPath);

        //\\//:
        Configuration conf = new Configuration();

        //\\//:
        // Create job:
        Job job = Job.getInstance(conf, "Compressible " + inputPath);
        job.setJarByClass(Compressible.class);

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
        job.setMapperClass(CompressibleMapper.class);
        job.setReducerClass(CompressibleReducer.class);

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
        int res = ToolRunner.run(new Configuration(), new Compressible(), args);
        System.exit(res);
    }
}
