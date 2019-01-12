package comp9313.ass4;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

/**
 * SingleSourceSP.
 * <p>
 * Find node's shortest path from a single source.
 */
public class SingleSourceSP {

    /**
     * Whether or not output the verbose detail when running the jobs.
     */
    public final static boolean VERBOSE = false;

    /**
     * Source node ID.
     */
    public final static String SOURCE_NAME = "source";

    /**
     * Mark whether the record is the original one.
     */
    public final static String ORIGINAL_MARK = "O";

    /**
     * Output folder.
     */
    public static String OUT = "output";

    /**
     * Input folder.
     */
    public static String IN = "input";

    /**
     * Count the change of minimum distance to source node.
     */
    public enum COUNTER {
        MIN_DISTANCE_CHANGED
    }

    /**
     * Map the input line into "from_node, to_node, distance" format.
     */
    public static class EdgeMapper extends Mapper<Object, Text, LongWritable, Text> {
        /**
         * Out key.
         */
        private LongWritable keyOut = new LongWritable();

        /**
         * Out value.
         */
        private Text valueOut = new Text();

        /**
         * Map the input string into "from_node, to_node, distance" format.
         *
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            // Skip comment
            if (line.startsWith("#")) {
                return;
            }

            String[] tokens = line.split("\\s+");

            int fromNode = Integer.valueOf(tokens[1]);
            String toNode = tokens[2];
            String distance = tokens[3];

            keyOut.set(fromNode);
            valueOut.set(toNode + ":" + distance);
            context.write(keyOut, valueOut);
        }
    }

    /**
     * Reduce by from node ID, construct adjacency list with distance.
     */
    public static class AdjacencyListReducer
            extends Reducer<LongWritable, Text, LongWritable, Text> {

        /**
         * Out key.
         */
        private LongWritable keyOut = new LongWritable();

        /**
         * Out value.
         */
        private Text valueOut = new Text();

        /**
         * Reduce by the from_node.
         *
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            /*
            // Sort adjacency list
            Map<Integer, Double> map = new TreeMap<>();
            for (Text value : values) {
                String[] pair = value.toString().split(":");
                int toNode = Integer.valueOf(pair[0]);
                double distance = Double.valueOf(pair[1]);
                map.put(toNode, distance);
            }

            // Convert adjacency list to string
            StringBuffer buffer = new StringBuffer();
            for (Map.Entry<Integer, Double> entry : map.entrySet()) {
                if (buffer.length() != 0) {
                    buffer.append(",");
                }

                buffer.append(String.format("%d:%f", entry.getKey(), entry.getValue()));
            }*/

            // Convert adjacency list to string
            StringBuffer buffer = new StringBuffer();
            for (Text value : values) {
                if (buffer.length() != 0) {
                    buffer.append(",");
                }

                buffer.append(value.toString());
            }

            // Distance to source
            long source = context.getConfiguration().getLong(SOURCE_NAME, 0);
            if (key.get() == source) {
                buffer.insert(0, String.format("%s ", 0.0));
            } else {
                buffer.insert(0, String.format("%s ", Double.POSITIVE_INFINITY));
            }

            keyOut.set(key.get());
            valueOut.set(buffer.toString());

            context.write(keyOut, valueOut);
        }
    }

    /**
     * A mapper, to spreading the path from the reachable nodes.
     */
    public static class SPMapper extends Mapper<Object, Text, LongWritable, Text> {

        /**
         * Out key.
         */
        private LongWritable keyOut = new LongWritable();

        /**
         * Out value.
         */
        private Text valueOut = new Text();

        /**
         * Spreading the path from the reachable nodes.
         *
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split("\\s+");

            // Node ID
            long node = Long.valueOf(tokens[0]);
            keyOut.set(node);

            if (tokens.length > 2) {
                valueOut.set(String.format("%s %s %s", tokens[1], tokens[2], ORIGINAL_MARK));
            } else {
                // no adjacency nodes, in other words no out edges
                valueOut.set(String.format("%s %s", tokens[1], ORIGINAL_MARK));
            }

            // Write the original data to stream
            context.write(keyOut, valueOut);

            // Distance to source
            double srcDist = Double.valueOf(tokens[1]);

            // Only handle the nodes, that reachable and has out edges
            if (srcDist != Double.POSITIVE_INFINITY && tokens.length > 2) {
                String[] adjacencyNodes = tokens[2].split(",");

                // For each adjacency node
                for (String adjacencyNode : adjacencyNodes) {
                    String[] pair = adjacencyNode.split(":");

                    int adjacencyNodeID = Integer.valueOf(pair[0]);
                    double adjacencyNodeDist = Double.valueOf(pair[1]);

                    // Spreading the reachable nodes
                    keyOut.set(adjacencyNodeID);
                    valueOut.set(String.valueOf(adjacencyNodeDist + srcDist));
                    context.write(keyOut, valueOut);
                }
            }
        }
    }


    /**
     * Reducer, to find the shortest path of a single node.
     */
    public static class SPReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        /**
         * Out value.
         */
        private Text valueOut = new Text();

        /**
         * Find the shortest path of the current node, to the source node.
         *
         * @param key     current node
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String adjacencyNodes = null;
            double originalDist = Double.POSITIVE_INFINITY;
            double minDist = Double.POSITIVE_INFINITY;

            // Find the minimum distance to source node
            for (Text value : values) {
                String record = value.toString();
                String[] tokens = record.split("\\s+");

                double distance = Double.valueOf(tokens[0]);

                // Original record
                if (tokens[tokens.length - 1].equals(ORIGINAL_MARK)) {
                    originalDist = distance;

                    // distance_to_src, adjacency_nodes, mark
                    if (tokens.length == 3) {
                        adjacencyNodes = tokens[1];
                    }
                }

                if (distance < minDist) {
                    minDist = distance;
                }
            }

            // If original distance and minimum distance are difference
            if (Math.abs(originalDist - minDist) > 1E-7) {
                context.getCounter(COUNTER.MIN_DISTANCE_CHANGED).increment(1);
            }

            if (adjacencyNodes == null) {
                // no adjacency nodes, in other words no out edges
                valueOut.set(String.format("%s", minDist));
            } else {
                valueOut.set(String.format("%s %s", minDist, adjacencyNodes));
            }

            context.write(key, valueOut);
        }
    }

    /**
     * Extract result from the output.
     */
    public static class ExtractMapper extends Mapper<Object, Text, LongWritable, DoubleWritable> {
        /**
         * Out key.
         */
        private LongWritable keyOut = new LongWritable();

        /**
         * Out value.
         */
        private DoubleWritable valueOut = new DoubleWritable();

        /**
         * Map the input text into the requirement format.
         *
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] tokens = line.split("\\s+");
            long toNodeID = Long.valueOf(tokens[0]);
            double minDist = Double.valueOf(tokens[1]);

            // Ignore infinity, the un-reachable vertex
            if (minDist == Double.POSITIVE_INFINITY) {
                return;
            }

            keyOut.set(toNodeID);
            valueOut.set(minDist);
            context.write(keyOut, valueOut);
        }
    }

    /**
     * Format the extracted result.
     */
    public static class ExtractReducer extends Reducer<LongWritable, DoubleWritable, Object, Text> {
        /**
         * Out value.
         */
        private Text valueOut = new Text();

        @Override
        protected void reduce(LongWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            // Get the minimum distance to source
            Iterator<DoubleWritable> iterator = values.iterator();
            double minDist = Double.POSITIVE_INFINITY;
            while (minDist == Double.POSITIVE_INFINITY && iterator.hasNext()) {
                minDist = iterator.next().get();
            }

            long nodeID = key.get();
            long source = context.getConfiguration().getLong(SOURCE_NAME, 0);
            valueOut.set(String.format("%d %d %f", source, nodeID, minDist));
            context.write(null, valueOut);
        }
    }


    /**
     * Main.
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        IN = args[0];
        OUT = args[1];
        long source = Long.valueOf(args[2]);

        // Input file name, without full path, without suffix, e.g. 'NA'
        String inFileName = new java.io.File(IN).getName().split("\\.")[0];
        // Temp file folder
        String tempFolder = String.format("/tmp/%s/", inFileName);

        String input = IN;
        String output = tempFolder + System.nanoTime();

        // Store source node ID into configuration
        Configuration conf = new Configuration();
        conf.setLong(SOURCE_NAME, source);

        // Convert the input file to the desired format for iteration
        Job convertJob = Job.getInstance(conf, "converter");
        convertJob.setJarByClass(SingleSourceSP.class);

        convertJob.setMapperClass(EdgeMapper.class);
        convertJob.setReducerClass(AdjacencyListReducer.class);

        convertJob.setOutputKeyClass(LongWritable.class);
        convertJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(convertJob, new Path(input));
        FileOutputFormat.setOutputPath(convertJob, new Path(output));

        convertJob.waitForCompletion(VERBOSE);


        // Loop until the minimum distance nothing change
        boolean isDone = false;

        while (isDone == false) {
            input = output;
            output = tempFolder + System.nanoTime();

            // Configure and run the MapReduce job
            Job shortestPathJob = Job.getInstance(conf, "iteration");

            shortestPathJob.setJarByClass(SingleSourceSP.class);

            shortestPathJob.setMapperClass(SPMapper.class);
            shortestPathJob.setReducerClass(SPReducer.class);

            shortestPathJob.setOutputKeyClass(LongWritable.class);
            shortestPathJob.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(shortestPathJob, new Path(input));
            FileOutputFormat.setOutputPath(shortestPathJob, new Path(output));

            shortestPathJob.waitForCompletion(VERBOSE);

            // When minimum distance nothing changed, is done
            isDone = shortestPathJob.getCounters()
                    .findCounter(COUNTER.MIN_DISTANCE_CHANGED).getValue() == 0;
        }

        // Extract the final result
        Job extractJob = Job.getInstance(conf, "extract");
        extractJob.setJarByClass(SingleSourceSP.class);

        extractJob.setMapperClass(ExtractMapper.class);
        extractJob.setReducerClass(ExtractReducer.class);
        extractJob.setNumReduceTasks(1);

        extractJob.setOutputKeyClass(LongWritable.class);
        extractJob.setOutputValueClass(DoubleWritable.class);

        input = output;
        FileInputFormat.addInputPath(extractJob, new Path(input));
        // Final result output folder
        output = String.format("%s%d", inFileName, source);
        if (OUT.endsWith("/")) {
            output = OUT + output;
        } else {
            output = OUT + "/" + output;
        }
        FileOutputFormat.setOutputPath(extractJob, new Path(output));

        System.exit(extractJob.waitForCompletion(VERBOSE) ? 0 : 1);
    }

}

