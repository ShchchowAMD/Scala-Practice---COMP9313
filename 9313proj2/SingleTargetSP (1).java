package comp9313.ass2;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Assignment 2: Single Target Shortest Path
 * 
 * @author Xingwei Chen
 * @Student_ID: z5105337
 */
public class SingleTargetSP {

	// global value IN, OUT, TARGET, corresponding to store the input configure
	// arg[0], arg[1], arg[2]
	public static String OUT = "output";
	public static String IN = "input";
	public static String TARGET = "target";
	
	// create a counter.
	public static enum MODIFY_COUNTER {
		UPDATE; // used to record the update time
	};

	/*
	 * Mapper class for preprocessing the input file.
	 */
	public static class PreMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// YOUR JOB: map function
			// EdgeId: 0 FromNodeId: 1 ToNodeId: 2 Distance: 3.0
			String line = value.toString();
			// splits by space
			String[] sp = line.split(" ");

			// reverse the graph set the target node as the single source node. Then reverse 
			// every edge set the toNode(sp[2]) as fromNode(sp[1]) , fromNode as toNode
			context.write(new Text(sp[2]), new Text(sp[1] + ":" + sp[3]));
		}
	}

	
	/*
	 * Reducer class for preprocessing the input file.
	 */
	public static class PreReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// create a String to stored the adjacency nodes information.
			StringBuilder sb = new StringBuilder();
			for (Text val : values) {
				// use ";" to separate the point:distance pair.
				if (sb.length() != 0) {
					sb.append(";");
				}
				sb.append(val.toString());
			}
			// initialize the inf with maximum value of Double, represent infinity.
			double inf = Double.MAX_VALUE;
			// if the key is equal to target value then the distance need to get
			// this point is 0, then assign 0 to inf.
			if (key.toString().equals(TARGET)) {
				inf = 0;
			}
			// Transform to the input format.
			// Input Format: SourceNode Distance (ToNode: distance; ToNode: distance; ...)
			context.write(key, new Text(inf + "\t" + sb.toString()));
		}
	}

	/*
	 * Mapper for calculate the shortest distance.
	 */
	public static class STMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// Input Format: SourceNode Distance (ToNode: distance; ToNode: distance; ...)
			String line = value.toString(); // looks like 0 0 4:2.0;2:5.0
			String[] sp = line.split("\t"); // split the line with "\t" signal.
			// get the cost to current node.
			double distance = Double.parseDouble(sp[1]);
			
			// check whether this node have adjacency nodes or not
			if(sp.length > 2){
				// split the adjacency list into point: distance_cost pairs.
				String[] PTDIS = sp[2].split(";");
				for (int i = 0; i < PTDIS.length; i++) {
					// split the point: distance pair
					String[] PTD = PTDIS[i].split(":");
					// get the accumulated distance value
					double add = Double.parseDouble(PTD[1]) + distance;
					// pass in the (node, value) pair
					context.write(new Text(PTD[0]), new Text("VALUE>" + add));
				}
				// pass in the adjacency node and its weight information.
				context.write(new Text(sp[0]), new Text("NODE>" + sp[2]));
			}
			// pass in current node's distance as the base to check whether there is an update.
			context.write(new Text(sp[0]), new Text("PVALUE>" + sp[1]));
		}
	}

	/*
	 * Reducer for calculate the shortest distance.
	 */
	public static class STReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// initialize the adjacency nodes
			String nodes = "";
			// create a list to store the distance value
			LinkedList<Double> list = new LinkedList<Double>();
			// initialize the min with maximum value of double
			double min = Double.MAX_VALUE;
			for (Text val : values) {
				// split the val by the signal.
				String[] sp = val.toString().split(">");
				if (sp[0].equals("NODE")) {
					// if this is equal to "NODE", then record the adjacency nodes.
					nodes = sp[1];
				} else if (sp[0].equals("PVALUE")) {
					// if this is equal to "PVALUE", then record the value
					min = Double.parseDouble(sp[1]);
				} else {
					// if equal to "VALUE", sorting it in the distance value list.
					double distance = Double.parseDouble(sp[1]);
					list.add(distance);
				}

			}

			for (double dis : list) {
				// check whether there are some value smaller than last iteration(Previous distance).
				if (min > dis) {
					min = dis;
					// count the update.
					context.getCounter(MODIFY_COUNTER.UPDATE).increment(1);
				}
			}
			// write the new (sourceNode, distance, AdjacenctList) tuple to the file.
			context.write(key, new Text(min + "\t" + nodes));
		}
	}

	/*
	 * Mapper class for process the result to satisfied the requirement.
	 */
	public static class EndMapper extends Mapper<Object, Text, IntWritable, Text> {

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// INPUT FORMAT: SourceNode: 0 Distance: 1 Adjacency List 2:3.0
			String line = value.toString();
			// splits by "\t"
			String[] sp = line.split("\t");
			// passing the information to reducer. (using MapReduce's default sort to sort the node)
			context.write(new IntWritable(Integer.parseInt(sp[0])), new Text(sp[1]));
		}
	}

	/*
	 * Reducer class for process the result to satisfied the requirement.
	 */
	public static class EndReducer extends Reducer<IntWritable, Text, Text, Text> {

		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// Output the final result to folder.
			for (Text val : values) {
				// if the distance between two node is infinity, skip it.
				if(Double.parseDouble(val.toString()) == Double.MAX_VALUE)
					continue;
				context.write(new Text(TARGET), new Text(key.toString() + "\t"
						+ val.toString()));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		IN = args[0];
		OUT = args[1];
		TARGET = args[2];

		// Initialize the input and output file
		String input = IN;
		String output = OUT + System.nanoTime();

		// YOUR JOB: Convert the input file to the desired format for iteration,
		// Transform to the input format.
		// Input Format: SourceNode Distance (ToNode: distance; ToNode:
		// distance; ...)
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Single Target Shortest Path");
		job.setJarByClass(SingleTargetSP.class);
		// Set the corresponding class for map and reduce.
		job.setMapperClass(PreMapper.class);
		job.setReducerClass(PreReducer.class);
		// Set the Data Type for mapper's output.
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// Set the Data Type for reducer's output.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// Set the input path and output path.
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);

		// Get the host name and port ID.
		String hostnamePort = "hdfs://localhost:9000";
		
		// Start to use BFS and MapReduce to calculate the shortest distance.
		boolean isdone = false;
		while(!isdone){
			// Update the input file and output file
			input = output;
			output = OUT + System.nanoTime();
			// YOUR JOB: Configure and run the MapReduce job
			// ... ...
			conf = new Configuration();
			job = Job.getInstance(conf, "Single Target Shortest Path");
			job.setJarByClass(SingleTargetSP.class);
			// Set the corresponding class for map and reduce.
			job.setMapperClass(STMapper.class);
			job.setReducerClass(STReducer.class);
			// Set the Data Type for mapper's output.
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			// Set the Data Type for reducer's output.
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			// Set the input path and output path.
			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));

			job.waitForCompletion(true);
			
			// Delete the output folder in the previous iteration to save disk space.
			FileSystem hdfs = FileSystem.get(
					URI.create(hostnamePort), conf);
			if (hdfs.exists(new Path(input))) {
				hdfs.delete(new Path(input), true);
			}
			
			// If the update time is 0, then stop the loop.
			Counters ct = job.getCounters(); // get the counter
			if (ct.findCounter(MODIFY_COUNTER.UPDATE).getValue() == 0) {
				isdone = true;
			}
		}

		// Extract the final result using another MapReduce job
		// with only 1 reducer, and store the results in HDFS
		conf = new Configuration();
		job = Job.getInstance(conf, "Single Target Shortest Path");
		job.setJarByClass(SingleTargetSP.class);
		// Set the corresponding class for map and reduce.
		job.setMapperClass(EndMapper.class);
		job.setReducerClass(EndReducer.class);
		// Set the Data Type for mapper's output.
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		// Set the Data Type for reducer's output.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		// Set the input path and output path.
		FileInputFormat.addInputPath(job, new Path(output));
		FileOutputFormat.setOutputPath(job, new Path(OUT));
		job.waitForCompletion(true);
		// Delete the intermediate output folder in the previous MapReduce work to save disk space.
		FileSystem hdfs = FileSystem.get(URI.create(hostnamePort),
				conf);
		if (hdfs.exists(new Path(output))) {
			hdfs.delete(new Path(output), true);
		}
	}
}
