/*
TallInfo.java
2012 Ⓒ CloudBrush, developed by Chien-Chih Chen (rocky@iis.sinica.edu.tw),
released under Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
at: https://github.com/ice91/CloudBrush

The file is derived from Contrail Project which is developed by Michael Schatz,
Jeremy Chambers, Avijit Gupta, Rushil Gupta, David Kelley, Jeremy Lewi,
Deepak Nettem, Dan Sommer, Mihai Pop, Schatz Lab and Cold Spring Harbor Laboratory,
and is released under Apache License 2.0 at:
http://sourceforge.net/apps/mediawiki/contrail-bio/
*/

package brush;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TailInfo {
    public String id;
    public String dir;
    public int dist;
    public int oval_size;

    public TailInfo(TailInfo o) {
        id = o.id;
        dir = o.dir;
        dist = o.dist;
        oval_size = o.oval_size;
    }

    public TailInfo() {
        id = null;
        dir = null;
        dist = 0;
        oval_size = 0;
    }

    public String toString() {
        if (this == null) {
            return "null";
        }
        return id + " " + dir + " " + dist;
    }

    public static TailInfo find_tail(Map<String, Node> nodes, Node startnode, String startdir) throws IOException {

        System.out.println("[**TailInfo.find_tail] input parameters are: nodes = " + nodes.toString() + " startnode: " + startnode.getNodeId() + " startdir: " + startdir);
        System.out.println("[**TailInfo.find_tail] starting with startnode..." + startnode.getNodeId() + " with startdir... " + startdir);

        Set<String> seen = new HashSet<String>();
        seen.add(startnode.getNodeId());
        System.out.println("[**TailInfo.find_tail] creating string set Set<String> seen = new HashSet<String>(); ...");
        System.out.println("[**TailInfo.find_tail] and add startnode " + startnode.getNodeId() + "as seen...");

        Node curnode = startnode;
        String curdir = startdir;
        String curid = startnode.getNodeId();
        int dist = 0;
        int oval_size = 0;

        System.out.println("[**TailInfo.find_tail] curnode: " + curnode + " curdir " + curdir + " curid " + curid + " dist " + dist + " oval_size " + oval_size);

        boolean canCompress = false;
        System.out.println("[**TailInfo.find_tail]  creating boolean canCompress as false");
        System.out.println("	[**TailInfo.find_tail]  **WHILE canCompress is false");
        do {
            canCompress = false;
            System.out.println("	[**TailInfo.find_tail]  canCompress=" + canCompress);

            System.out.println("	[**TailInfo.find_tail]  getting next tail of curnode " + curnode.getNodeId() + " by gettail...");
            TailInfo next = curnode.gettail(curdir);

            System.out.println("	[**TailInfo.find_tail] current node is: "+ curnode.getNodeId() + " curdir is: " + curdir + " and next is: " + next );
            if ((next != null) && (nodes.containsKey(next.id)) && (!seen.contains(next.id))) {
                System.out.println("	[**TailInfo.find_tail] if ((next != null) && (nodes.containsKey(next.id)) && (!seen.contains(next.id)))...");
                System.out.println("	[**TailInfo.find_tail] if ((" + next + " != null) && (nodes.containsKey("+next.id+")) && (!seen.contains("+next.id+")))...");

                seen.add(next.id);
                curnode = nodes.get(next.id); //\\//: here nodes.get(next.id) changes the fields value, but how???

                System.out.println("	[**TailInfo.find_tail] and add current node as seen...");
                System.out.println("	[**TailInfo.find_tail] current node is: "+ curnode.getNodeId());
                System.out.println("	[**TailInfo.find_tail] getting nexttail by gettail...");
                TailInfo nexttail = curnode.gettail(Node.flip_dir(next.dir));

                System.out.println("	[**TailInfo.find_tail] now current node is: "+ curnode.getNodeId() + " and nexttail is: " + nexttail );
                if ((curnode.getBlackEdges() == null) && (nexttail != null) && (nexttail.id.equals(curid))) {
                    System.out.println("		[**TailInfo.find_tail] if (curnode.getBlackEdges() == null) && (nexttail != null) && (nexttail.id.equals(curid))...");
                    System.out.println("		[**TailInfo.find_tail] if (" + curnode.getBlackEdges() + " == null) && ("+nexttail+" != null) && ("+nexttail.id+".equals("+curid+"))...");
                    dist++;
                    canCompress = true;
                    System.out.println("		[**TailInfo.find_tail] incrementing dist... dist=" + dist );
                    System.out.println("		[**TailInfo.find_tail] canCompress is " + canCompress );

                    curid = next.id;
                    curdir = next.dir;
                    oval_size = next.oval_size;
                    System.out.println("		[**TailInfo.find_tail] " + "curid: " + curid + " curdir: " + curdir + " oval_size: " + oval_size);
                }
            }
        } while (canCompress);
        System.out.println("	[**TailInfo.find_tail] **END WHILE LOOP canCompress is true now");

        System.out.println("[**TailInfo.find_tail] initiating new recursive TailInfo retval...");
        TailInfo retval = new TailInfo();

        System.out.println("[**TailInfo.find_tail] and assigning id, dir, dist, oval_size to retval...");
        retval.id = curid;
        retval.dir = Node.flip_dir(curdir);
        retval.dist = dist;
        retval.oval_size = oval_size;

        System.out.println("[**TailInfo.find_tail] "  + "curid "+ curid + " dir " + Node.flip_dir(curdir) + " dist " + dist + " oval_size " + oval_size);
        System.out.println("[**TailInfo.find_tail] return retval: " + retval + " retval.id "+ retval.id + " retval.dir " + retval.dir + " retval.dist " + retval.dist + " retval.oval_size " + retval.oval_size);

        return retval;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

}
