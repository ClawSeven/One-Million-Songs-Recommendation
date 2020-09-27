


import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;



public class SongMapReduce extends Configured implements Tool {

    /**
     *
     * Description : Mapper class that implements the map part of Breadth-first
     * search algorithm. The nodes colored WHITE or BLACK are emitted as such. For
     * each node that is colored GRAY, a new node is emitted with the distance
     * incremented by one and the color set to GRAY. The original GRAY colored
     * node is set to BLACK color and it is also emitted.
     *
     * Input format <key, value> : <line offset in the input file (automatically
     * assigned),
     * nodeID<tab>list_of_adjacent_nodes|distance_from_the_source|color|parent>
     *
     * Output format <key, value> : <nodeId, (updated)
     * list_of_adjacent_nodes|distance_from_the_source|color|parent node>
     *
     * Reference :
     * http://www.johnandcailin.com/blog/cailin/breadth-first-graph-search
     * -using-iterative-map-reduce-algorithm
     *
     */

    // the type parameters are the input keys type, the input values type, the
    // output keys type, the output values type
    public static class SongMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            Node inNode = new Node(value.toString());

            if (inNode.getColor() == Node.Color.GRAY) {
                for (String neighbor : inNode.getEdges()) {
                    Node adjacentNode = new Node();
                    adjacentNode.setId(neighbor);
                    adjacentNode.setDistance(inNode.getDistance() + 1);
                    adjacentNode.setColor(Node.Color.GRAY);

                    context.write(new Text(adjacentNode.getId()),
                            adjacentNode.getNodeInfo());
                }
                // this node is done, color it black
                inNode.setColor(Node.Color.BLACK);
            }

            context.write(new Text(inNode.getId()), inNode.getNodeInfo());
        }
    }

    static enum MoreIterations {
        numberOfIterations
    }

    /**
     *
     * Description : Reducer class that implements the reduce part of parallel
     * Breadth-first search algorithm. Make a new node which combines all
     * information for this single node id that is for each key. The new node
     * should have the full list of edges, the minimum distance, the darkest
     * Color, and the parent/predecessor node
     *
     * Input format <key,value> : <nodeId,
     * list_of_adjacent_nodes|distance_from_the_source|color|parent_node>
     *
     * Output format <key,value> : <nodeId,
     * (updated)list_of_adjacent_nodes|distance_from_the_source|color|parent_node>
     *
     */
    public static class SongReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // set the node id as the key
            Node outNode = new Node();
            outNode.setId(key.toString());

            for (Text value : values) {

                Node inNode = new Node(key.toString() + "\t" + value.toString());

                if (inNode.getEdges().size() > 0) {
                    outNode.setEdges(inNode.getEdges());
                }

                if (inNode.getDistance() < outNode.getDistance()) {
                    outNode.setDistance(inNode.getDistance());
                }

                if (inNode.getColor().ordinal() > outNode.getColor().ordinal()) {
                    outNode.setColor(inNode.getColor());
                }

            }

            if (outNode.getColor() == Node.Color.GRAY)
                context.getCounter(MoreIterations.numberOfIterations).increment(1L);

            context.write(key, new Text(outNode.getNodeInfo()));
        }
    }

    public int run(String[] args) throws Exception {
        int iterationCount = 0; // counter to set the ordinal number of the
        // intermediate outputs

        long terminationValue = 1;

        // while there are more gray nodes to process
        while (terminationValue > 0) {

            Job job = new Job(getConf());
            job.setJarByClass(SongMapReduce.class);
            job.setJobName("Artist Distance");
            job.setMapperClass(SongMapper.class);
            job.setReducerClass(SongReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            String input, output;

            // setting the input file and output file for each iteration
            // during the first time the user-specified file will be the input whereas
            // for the subsequent iterations
            // the output of the previous iteration will be the input
            if (iterationCount == 0) {
                // for the first iteration the input will be the first input argument
                input = args[0];
            } else {
                // for the remaining iterations, the input will be the output of the
                // previous iteration
                input = args[1] + iterationCount;
            }
            output = args[1] + (iterationCount + 1); // setting the output file

            FileInputFormat.setInputPaths(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));

            job.waitForCompletion(true);

            Counters jobCntrs = job.getCounters();
            terminationValue = jobCntrs
                    .findCounter(MoreIterations.numberOfIterations).getValue();
            iterationCount++;
            System.out.println("\n\nITERATION: "+iterationCount+"\t"+output+"\n\n");
            if  (iterationCount > 3888){
                System.out.println("\n\nBREAK!!!!!\n\n");
                break;
            }
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SongMapReduce(), args);
        if(args.length != 2){
            System.err.println("Usage: <in> <output name> ");
        }
        System.exit(res);
    }

}