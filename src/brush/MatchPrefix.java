/**
    MatchPrefix.java
    2012 Ⓒ CloudBrush, developed by Chien-Chih Chen (rocky@iis.sinica.edu.tw),
    released under Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
    at: https://github.com/ice91/CloudBrush
*/
/**
 * Modified by davbzh on 2016-12-16.
 */

package brush;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

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

public class MatchPrefix extends Configured implements Tool {

    private static final Logger sLogger = Logger.getLogger(MatchPrefix.class);

    public static class MatchPrefixMapper extends Mapper<LongWritable, Text, Text, Text> {

        //extract kmer from input
        int K = 0;
        //Insert size it is estimated from BWA alignment
        int insert_size = 0;

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String Kmer = conf.get("Kmer");
            String InsertSize = conf.get("InsertSize");
            insert_size = Integer.parseInt(InsertSize);
            K =  Integer.parseInt(Kmer);
        }

        public void map(LongWritable lineid, Text nodetxt, Context context) throws IOException, InterruptedException {

            //\\//: create new Node
            Node node = new Node();
            //\\//: populate the node from the incoming string
            node.fromNodeMsg(nodetxt.toString());

            String[] fields = nodetxt.toString().split("\t");
            String[] reads = fields[3].toString().split("!");
            String read_f = Node.dna2str(reads[0]);
            String read_r = Node.dna2str(reads[1]);

            //Gap between pair-end reads is calculated as
            int pair_gap = insert_size - (read_f.length() + read_r.length());
            int pair_distance = insert_size - read_f.length();
            int total_node_length = (read_f.length() + read_r.length()) + pair_gap;

            // As a first step lets start with brashes
            if (!node.hasCustom("n")) {

                //-----------------------------------
                //Start k-mer from forward ==> reverse:
                //start with prefix of f (/1) read
                String prefix_tmp = read_f.substring(0, K);
                String prefix = Node.str2dna(prefix_tmp);
                String prefix_r = Node.str2dna(Node.rc(prefix_tmp));

                //emit to reducers
                context.write(new Text(prefix), new Text(node.getNodeId() + "\t" + "ff1" + "\t" + node.toNodeMsg() + "\t"  + read_f.length()));
                context.write(new Text(prefix_r), new Text(node.getNodeId() + "\t" + "fr1" + "\t" + node.toNodeMsg() + "\t"  + read_f.length()));

                int end_f = read_f.length() - K;

                //Extract end of prefix of 1 (/1) read
                //Note that we will emit this as SUFFIX
                //TODO: Not sure if it needs to be emitted as PREFIX
                String end_prefix_tmp = read_f.substring(end_f);
                String end_prefix = Node.str2dna(Node.rc(end_prefix_tmp));
                String end_prefix_r = Node.str2dna(end_prefix_tmp);
                //emit to mapper
                context.write(new Text(end_prefix), new Text(node.getNodeId() + "\t" + "ff" + "\t" +
                        Node.SUFFIXMSG + "\t" + read_f.length() + "\t" + node.cov() + "\t" +
                        (read_f.length() + K)));
                context.write(new Text(end_prefix_r), new Text(node.getNodeId() + "\t" + "fr" + "\t" +
                        Node.SUFFIXMSG + "\t" + read_f.length() + "\t" + node.cov() + "\t" +
                        (read_f.length() + K)));

                System.out.println(" end_prefix: " + end_prefix_tmp + " Overlap_start: " + read_f.length());

                // slide the rest of the K-mer windows (suffixes) for each read in forward direction
                for (int i = 1; i < end_f; i++) {
                    //extract k-mer on f (/1) read
                    String f_window_tmp = read_f.substring(i, i + K);
                    String f_window = Node.str2dna(f_window_tmp);
                    String f_window_r = Node.str2dna(Node.rc(f_window_tmp));

                    //extract overlap start position
                    //int overlap_start_position_f = K + i;
                    int overlap_start_position_f = read_f.length() - i;

                    System.out.println("f_window_tmp: " + f_window_tmp + " overlap_start_position_f: " + overlap_start_position_f);

                    //emit to mapper
                    context.write(new Text(f_window), new Text(node.getNodeId() + "\t" + "ff" + "\t" +
                            Node.SUFFIXMSG + "\t" + overlap_start_position_f + "\t" + node.cov() + "\t" +
                            (read_f.length() + K)));
                    context.write(new Text(f_window_r), new Text(node.getNodeId() + "\t" + "fr" + "\t" +
                            Node.SUFFIXMSG + "\t" + overlap_start_position_f + "\t" + node.cov() + "\t" +
                            (read_f.length() + K)));

                    //extract k-mer on r (/2) read and reverse complement each of them
                    String r_window_tmp = read_r.substring(i, i + K);
                    String r_window_r = Node.str2dna(Node.rc(r_window_tmp));
                    String r_window_f = Node.str2dna(r_window_tmp);

                    //extract overlap start position
                    //int overlap_start_position_r = (read_f.length() + pair_gap) + K + i;
                    int overlap_start_position_r = (read_f.length() + pair_gap + read_r.length()) - ((read_f.length() +
                            pair_gap + read_r.length()) - K - i);

                    System.out.println("r_window_tmp: " + r_window_tmp + " overlap_start_position_r: " + overlap_start_position_r);

                    //emit to mapper
                    context.write(new Text(r_window_r), new Text(node.getNodeId() + "\t" + "rr" +
                            "\t" + Node.SUFFIXMSG + "\t" + overlap_start_position_r + "\t" + node.cov() +
                            "\t" + (total_node_length + K)));
                    context.write(new Text(r_window_f), new Text(node.getNodeId() + "\t" + "rf" + "\t" +
                            Node.SUFFIXMSG + "\t" + overlap_start_position_r + "\t" + node.cov() + "\t" +
                            (total_node_length + K)));
                }

                //Extract start of prefix of r (/2) read
                //Note that we will emit this as SUFFIX
                //TODO: Not sure if it needs to be emitted as PREFIX
                String start_prefix_rc_tmp = read_r.substring(0, K);
                String start_prefix_rc = Node.str2dna(start_prefix_rc_tmp);
                String start_prefix_rf = Node.str2dna(Node.rc(start_prefix_rc_tmp));
                int start_overlap_start_position_r = (read_f.length() + pair_gap + read_r.length()) - ((read_f.length() +
                        pair_gap + read_r.length()) - K - 0);                //emit to mapper
                context.write(new Text(start_prefix_rc), new Text(node.getNodeId() + "\t" + "rr" +
                        "\t" + Node.SUFFIXMSG + "\t" + start_overlap_start_position_r + "\t" + node.cov() +
                        "\t" + (total_node_length + K)));
                context.write(new Text(start_prefix_rf), new Text(node.getNodeId() + "\t" + "rf" + "\t" +
                        Node.SUFFIXMSG + "\t" + start_overlap_start_position_r + "\t" + node.cov() + "\t" +
                        (total_node_length + K)));

                System.out.println(" start_prefix_rc: " + start_prefix_rc_tmp + " Overlap_start: " + start_overlap_start_position_r);

                //end with prefix of r (/2) read
                String prefix_rc_tmp = read_r.substring(read_r.length() - K);
                String prefix_rc = Node.str2dna(Node.rc(prefix_rc_tmp));
                String prefix_rf = Node.str2dna(prefix_rc_tmp);

                //emit to mapper
                context.write(new Text(prefix_rc), new Text(node.getNodeId() + "\t" + "rr2" + "\t" + node.toNodeMsg() + "\t"  + read_r.length()));
                context.write(new Text(prefix_rf), new Text(node.getNodeId() + "\t" + "rf2" + "\t" + node.toNodeMsg() + "\t"  + read_r.length()));
            }
        }
    }

    public static class MatchPrefixReducer extends Reducer<Text, Text, Text, Text> {

        public class EdgeInfo {

            public String id;
            public String edge_type;
            public int overlap_start_position;
            public float cov;

            public EdgeInfo(String id1, String edge_type1, int overlap_start_position1, float cov1) throws IOException {

                id = id1;
                edge_type = edge_type1;
                overlap_start_position = overlap_start_position1;
                cov = cov1;
            }

            public String toString() {
                return id + "!" + overlap_start_position + "|" + cov;
            }
        }

        class OvelapSizeComparator implements Comparator {
            public int compare(Object element1, Object element2) {
                EdgeInfo obj1 = (EdgeInfo) element1;
                EdgeInfo obj2 = (EdgeInfo) element2;
                if ((int) (obj1.overlap_start_position - obj2.overlap_start_position) >= 0) {
                    return -1;
                } else {
                    return 1;
                }
            }
        }

        public void reduce(Text prefix, Iterable<Text> iter, Context context) throws IOException, InterruptedException {
            Map<String, Map<String, Node>> idx_nodes = new HashMap<String, Map<String, Node>>();
            Map<String, Node> nodes;
            Map<String, Node> emit_nodes = new HashMap<String, Node>();
            Map<String, List<EdgeInfo>> idx_elist = new HashMap<String, List<EdgeInfo>>();
            List<EdgeInfo> elist;

            int prefix_sum = 0;
            int kmer_count = 0;

            for (Text msg : iter) {
                String[] vals = msg.toString().split("\t"); // \\//: tokenize it
                float tmp_cov = 0;
                if (vals[2].equals(Node.NODEMSG)) {
                    Node node = new Node(vals[0]);
                    node.parseNodeMsg(vals, 2);
                    String edge_type = vals[1].substring(0, 2);
                    String rev_idx = vals[1].substring(2, 3);
                    if (rev_idx.equals("1")) {
                        if (idx_nodes.containsKey(prefix.toString())) {
                            nodes = idx_nodes.get(prefix.toString());
                            nodes.put(node.getNodeId() + "|" + edge_type, node);
                            idx_nodes.put(prefix.toString(), nodes);
                        } else {
                            nodes = new HashMap<String, Node>();
                            nodes.put(node.getNodeId() + "|" + edge_type, node);
                            idx_nodes.put(prefix.toString(), nodes);
                        }
                    }  else if (rev_idx.equals("2")) {
                        if (idx_nodes.containsKey(prefix.toString())) {
                            nodes = idx_nodes.get(prefix.toString());
                            nodes.put(node.getNodeId() + "|" + edge_type, node);
                            idx_nodes.put(prefix.toString(), nodes);
                        } else {
                            nodes = new HashMap<String, Node>();
                            nodes.put(node.getNodeId() + "|" + edge_type, node);
                            idx_nodes.put(prefix.toString(), nodes);
                        }
                    } else {
                        //TODO: here I need catch error
                        System.out.println("	    ERROR: Unknow NODE!!!");
                    }

                    EdgeInfo ei = new EdgeInfo(vals[0], vals[1].substring(0, 2), Integer.parseInt(vals[7]), node.cov());
                    if (idx_elist.containsKey(prefix.toString())) {
                        elist = idx_elist.get(prefix.toString());
                        elist.add(ei);
                        idx_elist.put(prefix.toString(), elist);
                    } else {
                        elist = new ArrayList<EdgeInfo>();
                        elist.add(ei);
                        idx_elist.put(prefix.toString(), elist);
                    }

                } else if (vals[2].equals(Node.SUFFIXMSG)) {
                    tmp_cov = Float.parseFloat(vals[4]);
                    EdgeInfo ei = new EdgeInfo(vals[0], vals[1], Integer.parseInt(vals[3]), Float.parseFloat(vals[4]));
                    if (idx_elist.containsKey(prefix.toString())) {
                        elist = idx_elist.get(prefix.toString());
                        elist.add(ei);
                        idx_elist.put(prefix.toString(), elist);

                        System.out.println( "prefixincoming kmer " +  Node.dna2str(prefix.toString()) + "to idx_elist: " + idx_elist.toString());

                    } else {
                        elist = new ArrayList<EdgeInfo>();
                        elist.add(ei);
                        idx_elist.put(prefix.toString(), elist);

                        System.out.println( "prefixincoming kmer " +  Node.dna2str(prefix.toString()) + "to idx_elist: " + idx_elist.toString());

                    }
                } else {
                    throw new IOException("Unknown msgtype: " + msg);
                }
                kmer_count = kmer_count + (int) tmp_cov;
                prefix_sum = prefix_sum + (int) tmp_cov;
            }

            System.out.println("idx_elist_after_loop: " + idx_elist.toString());

            for (String idx : idx_nodes.keySet()) {
                elist = idx_elist.get(idx);
                nodes = idx_nodes.get(idx);

                Map<String, List<String>> edges_list = new HashMap<String, List<String>>();
                Collections.sort(elist, new OvelapSizeComparator());
                for (int i = 0; i < elist.size() /*&& i < HighKmer*/; i++) {
                    if (edges_list.containsKey(elist.get(i).edge_type)) {
                        edges_list.get(elist.get(i).edge_type).add(elist.get(i).toString());
                    } else {
                        List<String> tmp_edges = null;
                        tmp_edges = new ArrayList<String>();
                        tmp_edges.add(elist.get(i).toString());
                        edges_list.put(elist.get(i).edge_type, tmp_edges);
                    }
                    context.getCounter("K-mer frequency filter", "nodecount").increment(1);
                }

                System.out.println("edges_list: " + edges_list.toString());

                //\\//: loop through node ids
                for (String nodeid_dir : nodes.keySet()) {
                    String dir = nodeid_dir.substring(nodeid_dir.indexOf("|") + 1); // @see https://docs.oracle.com/javase/7/docs/api/java/lang/String.html.
                    Node node_idx = nodes.get(nodeid_dir);
                    Node node = emit_nodes.get(node_idx.getNodeId());
                    if (node == null) {
                        node = node_idx;
                    }

                    for (String adj : Node.edgetypes) {
                        List<String> edges = edges_list.get(adj); //\\//: get forward or reverse edges
                        if (edges != null) {
                            context.getCounter("NON NULL", "nodecount").increment(1);
                            String key = dir;
                            for (int i = 0; i < edges.size(); i++) {
                                String edge_id = edges.get(i).substring(0, edges.get(i).indexOf("!"));
                                int overlap_start_position = Integer.parseInt(edges.get(i).substring(edges.get(i).indexOf("!")
                                        + 1, edges.get(i).indexOf("|")));
                                //float cov = Float.parseFloat(edges.get(i).substring(edges.get(i).indexOf("|") + 1));

                                System.out.println( "edge_id: " +  edge_id + " overlap_start_position: "
                                        + overlap_start_position);

                                if (node.getNodeId().equals(edge_id)) {

                                    System.out.println( "NodeId: "+node.getNodeId()+" == edge_id " + edge_id);

                                    context.getCounter("edge equals to", "nodecount").increment(1);
                                } else {

                                    System.out.println( "NodeId: "+node.getNodeId()+" != edge_id " + edge_id);

                                    context.getCounter("edge not equals to", "nodecount").increment(1);
                                    if (!node.hasEdge(key, edge_id, overlap_start_position)) { //\\//: and node doesn't have the specific edge
                                        context.getCounter("doensn't have edge", "nodecount").increment(1);
                                        node.addEdge(key, edges.get(i).substring(0, edges.get(i).indexOf("|"))); //\\//: add this edge
                                    } else {
                                        context.getCounter("has edge", "nodecount").increment(1);
                                    }
                                }
                            }
                        } else {
                            context.getCounter("NULL", "nodecount").increment(1);
                        }
                    }

                    emit_nodes.put(node.getNodeId(), node);
                    context.getCounter("Brush", "nodecount").increment(1);
                }
            }

            for (String id : emit_nodes.keySet()) {
                Node node = emit_nodes.get(id);
                context.write(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
            }

        }
    }

    //\\//:
    public int run(String inputPath, String outputPath, String Kmer, String InsertSize ) throws Exception {
        sLogger.info("Tool name: MatchPrefix");
        sLogger.info(" - input: " + inputPath);
        sLogger.info(" - output: " + outputPath);
        sLogger.info(" - Kmer_length: " + Kmer);
        sLogger.info(" - InsertSize: " + InsertSize);

        //\\//:
        Configuration conf = new Configuration();

        //Strore Kmer value to use it in mapreduce
        conf.set("Kmer", Kmer);
        conf.set("InsertSize", InsertSize);

        //\\//:
        int K = Integer.parseInt(Kmer);

        // Create job:
        Job job = Job.getInstance(conf, "MatchPrefix " + inputPath + " " + K);
        job.setJarByClass(MatchPrefix.class);

        // Setup input and output paths
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        //\\//:
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //\\//:
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Specify the type of output keys and values
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Setup MapReduce job
        job.setMapperClass(MatchPrefixMapper.class);
        job.setReducerClass(MatchPrefixReducer.class);

        // delete the output directory if it exists already
        FileSystem.get(conf).delete(new Path(outputPath), true);

        // \\//:
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public int run(String[] args) throws Exception {

        String inputPath = args[0];
        String outputPath = args[1];
        String Kmer = args[2];
        String InsertSize = args[3];

        long starttime = System.currentTimeMillis();

        run(inputPath, outputPath, Kmer, InsertSize);

        long endtime = System.currentTimeMillis();

        float diff = (float) (((float) (endtime - starttime)) / 1000.0);

        System.out.println("Runtime: " + diff + " s");

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MatchPrefix(), args);
        System.exit(res);
    }

}
