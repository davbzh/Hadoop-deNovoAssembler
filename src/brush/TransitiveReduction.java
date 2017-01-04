package brush;
/*
TransitiveReduction.java
2012 Ⓒ CloudBrush, developed by Chien-Chih Chen (rocky@iis.sinica.edu.tw),
released under Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
at: https://github.com/ice91/CloudBrush
*/
/**
 * Modified by davbzh on 2016-12-17.
 */

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.ArrayList;
import java.util.HashMap;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.log4j.Logger;

public class TransitiveReduction extends Configured implements Tool {
    private static final Logger sLogger = Logger.getLogger(TransitiveReduction.class);

    ///////////////////////////////////////////////////////////////////////////
    // TransitiveReductionMapper
    public static class TransitiveReductionMapper extends Mapper<LongWritable, Text, Text, Text>  {

        public void map(LongWritable lineid, Text nodetxt, Context context) throws IOException, InterruptedException {
            Node node = new Node();
            node.fromNodeMsg(nodetxt.toString());

            for (String key : Node.edgetypes) {
                List<String> edges = node.getEdges(key);
                if (edges != null) {

                    for (int i = 0; i < edges.size(); i++) {
                        String[] vals = edges.get(i).split("!");
                        String edge_id = vals[0];
                        String oval_size = vals[1];

                        //String con = Node.flip_link(key);
                        String con = key;
                        //Emit to reducer
                        context.write(new Text(edge_id), new Text(Node.OVALMSG + "\t" + node.getNodeId() + "\t" + node.str_raw() + "\t" + con + "\t" + oval_size));
                    }
                }
            }
            List<String> emit_node = new ArrayList<String>();

            //Emit to reducer
            context.write(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
            context.getCounter("Brush", "nodes").increment(1);
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // TransitiveReductionReducer
    public static class TransitiveReductionReducer extends Reducer<Text, Text, Text, Text> {

        static public float ERRORRATE = 0.00f;
        static public int Insert_size = 0;

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String ERROR_RATE = conf.get("err_rate");
            String InsertSize = conf.get("InsertSize");
            ERRORRATE =  Float.parseFloat(ERROR_RATE);
            Insert_size = Integer.parseInt(InsertSize);
        }

        public class OverlapInfo {
            public String id;
            public String str;
            public String edge_type;
            public int overlap_size;

            //Separate f and r reads
            String[] reads;
            public String read_f;
            public String read_r;

            public OverlapInfo(String[] vals, int offset) throws IOException {

                if (!vals[offset].equals(Node.OVALMSG)) {
                    throw new IOException("Unknown message type");
                }

                id = vals[offset + 1];
                str = vals[offset + 2];
                edge_type = vals[offset + 3];
                overlap_size = Integer.parseInt(vals[offset + 4]);

                reads = str.toString().split("!");
                read_f = reads[0];
                read_r = reads[1];
                if (edge_type.equals("ff")) {
                    str = read_f;
                } else if (edge_type.equals("fe")) {
                    str = read_f;
                } else if (edge_type.equals("rr")) {
                    str = read_r;
                } else if (edge_type.equals("re")) {
                    str = read_r;
                } else {
                    //TODO: I need to also check for fr and rf orientations
                    throw new IOException("ERROR: Unknown edge type " + edge_type);
                }
            }
            public String toString() {
                return edge_type + " " + id + " " + overlap_size + " " + str;
            }
        }

        class OvelapSizeComparator_f implements Comparator {
            public int compare(Object element1, Object element2) {
                OverlapInfo obj1 = (OverlapInfo) element1;
                OverlapInfo obj2 = (OverlapInfo) element2;
                if ((int) (obj1.overlap_size - obj2.overlap_size) > 0) {
                    return -1;
                } else if ((int) (obj1.overlap_size - obj2.overlap_size) < 0) {
                    return 1;
                } else {
                    if (obj1.str.length() - obj2.str.length() < 0) {
                        return -1;
                    } else if (obj1.str.length() - obj2.str.length() > 0) {
                        return 1;
                    } else {
                        if (obj1.id.compareTo(obj2.id) < 0) {
                            return -1;
                        } else {
                            return 1;
                        }
                    }
                }
            }
        }

        class OvelapSizeComparator_r implements Comparator {
            public int compare(Object element1, Object element2) {
                OverlapInfo obj1 = (OverlapInfo) element1;
                OverlapInfo obj2 = (OverlapInfo) element2;
                if ((int) (obj1.overlap_size - obj2.overlap_size) > 0) {
                    return -1;
                } else if ((int) (obj1.overlap_size - obj2.overlap_size) < 0) {
                    return 1;
                } else {
                    if (obj1.str.length() - obj2.str.length() < 0) {
                        return -1;
                    } else if (obj1.str.length() - obj2.str.length() > 0) {
                        return 1;
                    } else {
                        if (obj1.id.compareTo(obj2.id) < 0) {
                            return 1;
                        } else {
                            return -1;
                        }
                    }
                }
            }
        }

        public class Prefix {
            public String id;
            public String suffix;
            public String str;
            public String edge_type;
            public int oval_size;

            public Prefix(String id1, String edge_type1, String str1, int oval_size1) {
                id = id1;
                edge_type = edge_type1;
                str = str1;
                oval_size = oval_size1;
                suffix = str1.substring(oval_size);
            }
        }

        //\\//:
        public void reduce(Text nodeid, Iterable<Text> iter,  Context context) throws IOException, InterruptedException {
            Node node = new Node(nodeid.toString());
            List<OverlapInfo> o_flist = new ArrayList<OverlapInfo>();
            List<OverlapInfo> o_rlist = new ArrayList<OverlapInfo>();

            int sawnode = 0;

            for(Text msg : iter) {

                String [] vals = msg.toString().split("\t"); //\\//: tokenize it
                if (vals[0].equals(Node.NODEMSG)) {
                    node.parseNodeMsg(vals, 0);
                    sawnode++;
                } else if (vals[0].equals(Node.OVALMSG)) {
                    OverlapInfo oi = new OverlapInfo(vals, 0);
                    if (oi.edge_type.charAt(0) == 'f') {
                        o_flist.add(oi);
                    } else if (oi.edge_type.charAt(0) == 'r') {
                        o_rlist.add(oi);
                    }
                } else {
                    throw new IOException("Unknown msgtype: " + msg);
                }
            }
            // \\ one node
            if (sawnode != 1) {
                throw new IOException("ERROR: Didn't see exactly 1 nodemsg (" + sawnode + ") for " + nodeid.toString());
            }

            // \\ store confirmed edges
            Map<String, List<String>> edges_list = new HashMap<String, List<String>>();
            Map<String, List<String>> IDs_flist = new HashMap<String, List<String>>();
            Map<String, List<String>> IDs_rlist = new HashMap<String, List<String>>();
            Map<String, List<Prefix>> PREFIXs_list = new HashMap<String, List<Prefix>>();

            // \\\\\\\\\\\\\\\\\\\\\\\\\\ f_overlap
            int f_choices = o_flist.size();
            if (f_choices > 0) {
                // Sort overlap strings in order of decreasing overlap size
                Collections.sort(o_flist, new OvelapSizeComparator_f());

                // See if there are any pairwise compatible strings
                for (int i = 0; i < f_choices; i++) {
                    String oval_id = o_flist.get(i).id;
                    String oval_type = o_flist.get(i).edge_type;
                    // String node_dir = oval_type.substring(0, 1);
                    String oval_dir = oval_type.substring(1);
                    int oval_size = o_flist.get(i).overlap_size;

                    String edge_content = oval_id + "!" + oval_size;
                    String oval_seq_tmp = Node.dna2str(o_flist.get(i).str);
                    String oval_seq;
                    //-----------------------------------------------------
                    //TODO: remove?
                    if (oval_dir.equals("r")) {
                        oval_seq = Node.rc(oval_seq_tmp);
                    } else {
                        oval_seq = oval_seq_tmp;
                    }
                    //------------------------------------------------------

                    //--------------------------------------------------------------------------------------
                    //TODO: remove!
                    // \\ Self contained filter
                    if (oval_size == oval_seq.length() && oval_size == node.str().length()) {
                        context.getCounter("Brush", "contained_edge").increment(1);
                    }
                    /*
                    // \\\\\\\\\\\ Maximal Overlap filter
                    List<String> stored_IDs = IDs_flist.get(oval_type);
                    boolean has_large_overlap = false;
                    if (stored_IDs != null && stored_IDs.contains(oval_id)) {
                        has_large_overlap = true;
                    }
                    if (has_large_overlap) {
                        node.addRemovalEdge(oval_id, Node.flip_link(oval_type), node.getNodeId(), oval_size);
                        continue;
                    }
                    System.out.println("has_large_overlap: " + has_large_overlap);
                    */
                    //---------------------------------------------------------------------------------------

                    // \\\\\\\\\\\ Transitive Reduction filter
                    // Let X → Y → Z be such a path. The string corresponding to this path is a valid assembly of
                    // the three reads which is identical to the string corresponding to the path X → Z. In this case,
                    // we say that the edge X ↔ Z is transitive.
                    //TODO: Check this on the test file

                    List<Prefix> stored_PREFIXs = PREFIXs_list.get("f");
                    String prefix = oval_seq.substring(oval_size);

                    boolean has_trans_edge = false;
                    for (int j = 0; stored_PREFIXs != null && j < stored_PREFIXs.size(); j++) {
                        if (stored_PREFIXs.get(j).oval_size == oval_size
                                && stored_PREFIXs.get(j).str.length() == oval_seq.length()) {
                            continue;
                        }
                        if (ERRORRATE <= 0) {
                            if (prefix.startsWith(stored_PREFIXs.get(j).suffix)) {
                                context.getCounter("Brush", "trans_edge").increment(1);
                                has_trans_edge = true;
                                node.addRemovalEdge(oval_id, Node.flip_link(oval_type), node.getNodeId(), oval_size);
                                break;
                            }
                        }
                    }
                    if (has_trans_edge) {
                        continue;
                    }

                    // \\\\\\\\\\\\\ Store confirmed edge
                    if (PREFIXs_list.containsKey("f")) {
                        PREFIXs_list.get("f").add(new Prefix(oval_id, oval_type, oval_seq, oval_size));
                    } else {
                        List<Prefix> tmp_PREFIXs = new ArrayList<Prefix>();
                        tmp_PREFIXs.add(new Prefix(oval_id, oval_type, oval_seq, oval_size));
                        PREFIXs_list.put("f", tmp_PREFIXs);
                    }
                    if (edges_list.containsKey(oval_type)) {
                        edges_list.get(oval_type).add(edge_content);
                        IDs_flist.get(oval_type).add(oval_id);
                    } else {
                        List<String> tmp_edges = null;
                        tmp_edges = new ArrayList<String>();
                        tmp_edges.add(edge_content);
                        edges_list.put(oval_type, tmp_edges);
                        List<String> tmp_IDs = new ArrayList<String>();
                        tmp_IDs.add(oval_id);
                        IDs_flist.put(oval_type, tmp_IDs);
                    }

                    System.out.println("IDs_flist: " + IDs_flist.toString() + " edges_list: " +  edges_list.toString());

                }
            }

            //--------------------------------------------
            //TODO: remove!! This is just debugger
            String listString = "";
            for (int i = 0; i < PREFIXs_list.size(); i++) {
                for (Prefix s : PREFIXs_list.get("f")) {
                    listString += s.id + "\t" + s.suffix + "\t" + s.str + "\t" + s.edge_type + "\t" + s.oval_size + "\t" + "\t";
                }
            }

            System.out.println("f_PREFIXs_list: " + listString.toString());
            //--------------------------------------------

            // \\\\\\\\\\\\\\\\\\\\\\\\\\ r_overlap
            int r_choices = o_rlist.size();
            if (r_choices > 0) {
                // Sort overlap strings in order of decreasing overlap size
                Collections.sort(o_rlist, new OvelapSizeComparator_r());
                // See if there are any pairwise compatible strings
                for (int i = 0; i < r_choices; i++) {
                    String oval_id = o_rlist.get(i).id;
                    String oval_type = o_rlist.get(i).edge_type;
                    String oval_dir = oval_type.substring(1);
                    int oval_size = o_rlist.get(i).overlap_size;
                    String edge_content = oval_id + "!" + oval_size;
                    String oval_seq_tmp = Node.dna2str(o_rlist.get(i).str);
                    String oval_seq;

                    //-------------------------------------------------------------------------
                    //TODO: remove?
                    if (oval_dir.equals("r")) {
                        oval_seq = Node.rc(oval_seq_tmp);
                    } else {
                        oval_seq = oval_seq_tmp;
                    }
                    //-------------------------------------------------------------------------

                    //-------------------------------------------------------------------------
                    //TODO: remove!
                    // \\ Self contained filter
                    if (oval_size == oval_seq.length() && oval_size == node.str().length()) {
                        context.getCounter("Brush", "contained_edge").increment(1);
                    }
                    /*
                    // \\\\\\\\\\\ Maximal Overlap filter
                    List<String> stored_IDs = IDs_rlist.get(oval_type);
                    boolean has_large_overlap = false;
                    if (stored_IDs != null && stored_IDs.contains(oval_id)) {
                        has_large_overlap = true;
                    }
                    if (has_large_overlap) {
                        //node.addRemovalEdge(oval_id, Node.flip_link(oval_type), node.getNodeId(), oval_size);
                        node.addRemovalEdge(oval_id, oval_type, node.getNodeId(), oval_size);
                        continue;
                    }
                    */
                    //-------------------------------------------------------------------------

                    // \\\\\\\\\\\ Transitive Reduction filter
                    List<Prefix> stored_PREFIXs = PREFIXs_list.get("r");
                    String prefix = oval_seq.substring(oval_size);
                    boolean has_trans_edge = false;

                    for (int j = 0; stored_PREFIXs != null && j < stored_PREFIXs.size(); j++) {
                        if (stored_PREFIXs.get(j).oval_size == oval_size
                                && stored_PREFIXs.get(j).str.length() == oval_seq.length()) {
                            continue;
                        }
                        if (ERRORRATE <= 0) {
                            if (prefix.startsWith(stored_PREFIXs.get(j).suffix)) {
                                context.getCounter("Brush", "trans_edge").increment(1);
                                has_trans_edge = true;
                                node.addRemovalEdge(oval_id, Node.flip_link(oval_type), node.getNodeId(), oval_size);
                                break;
                            }
                        }

                    }
                    if (has_trans_edge) {
                        continue;
                    }

                    // \\\\\\\\\\\\\ Store confirmed edge
                    if (PREFIXs_list.containsKey("r")) {
                        PREFIXs_list.get("r").add(new Prefix(oval_id, oval_type, oval_seq, oval_size));
                    } else {
                        List<Prefix> tmp_PREFIXs = new ArrayList<Prefix>();
                        tmp_PREFIXs.add(new Prefix(oval_id, oval_type, oval_seq, oval_size));
                        PREFIXs_list.put("r", tmp_PREFIXs);
                    }

                    //--------------------------------------------
                    //TODO: remove!! This is just debugger
                    String r_listString = "";
                    for (int k = 0; k < PREFIXs_list.size(); k++) {
                        for (Prefix s : PREFIXs_list.get("r")) {
                            r_listString += s.id + "\t" + s.suffix + "\t" + s.str + "\t" + s.edge_type + "\t" + s.oval_size + "\t" + "\t";
                        }
                    }
                    System.out.println("r_PREFIXs_list: " + r_listString);
                    //--------------------------------------------

                    if (edges_list.containsKey(oval_type)) {
                        edges_list.get(oval_type).add(edge_content);
                        IDs_rlist.get(oval_type).add(oval_id);
                    } else {
                        List<String> tmp_edges = null;
                        tmp_edges = new ArrayList<String>();
                        tmp_edges.add(edge_content);
                        edges_list.put(oval_type, tmp_edges);
                        List<String> tmp_IDs = new ArrayList<String>();
                        tmp_IDs.add(oval_id);
                        IDs_rlist.put(oval_type, tmp_IDs);
                    }

                    System.out.println("IDs_rlist: " + IDs_rlist.toString() + " edges_list: " +  edges_list.toString());

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

    //\\//:
    //public RunningJob run(String inputPath, String outputPath) throws Exception
    public int run(String inputPath, String outputPath, String ERRORRATE, String InsertSize ) throws Exception {
        sLogger.info("Tool name: TransitiveReduction");
        sLogger.info(" - input: " + inputPath);
        sLogger.info(" - output: " + outputPath);
        sLogger.info(" - ERRORRATE: " + ERRORRATE);
        sLogger.info(" - InsertSize: " + InsertSize);

        //\\//:
        //JobConf conf = new JobConf(TransitiveReduction.class);
        Configuration conf = new Configuration();

        //Strore Kmer value to use it in mapreduce
        conf.set("err_rate", ERRORRATE);
        conf.set("InsertSize", InsertSize);

        //\\//:
        // Create job:
        Job job = Job.getInstance(conf, "TransitiveReduction " + inputPath );
        job.setJarByClass(TransitiveReduction.class);

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

        // conf.setBoolean("mapred.output.compress", true);

        //\\//:
        job.setMapperClass(TransitiveReductionMapper.class);
        job.setReducerClass(TransitiveReductionReducer.class);

        // delete the output directory if it exists already
        FileSystem.get(conf).delete(new Path(outputPath), true);

        //\\//:
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public int run(String[] args) throws Exception {
        String inputPath =  args[0];
        String outputPath = args[1];
        String ERRORRATE = args[2];
        String InsertSize  = args[3];

        run(inputPath, outputPath, ERRORRATE, InsertSize);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TransitiveReduction(), args);
        System.exit(res);
    }
}
