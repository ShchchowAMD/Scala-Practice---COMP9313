package comp9313.ass2;

//import ...
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress.Counter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SingleTargetSP {

  public static String OUT = "output";
  public static String IN = "input";
  public static int queryNodeID;
  
  //set the enum COUNTER
  static enum eInf {
	  COUNTER
  }

  //the AdjacencyListMapper and AdjacencyListReducer are used to deal with the txt files
  public static class AdjacencyListMapper extends Mapper<Object, Text, IntWritable, Text> {
	  //create text parameters id,fromNode,toNode and distance
	  private Text id = new Text();
      private Text fromNode = new Text();
      private Text toNode = new Text();
      private Text distance = new Text();
          
      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
          String line = value.toString();
          StringTokenizer tokenizer = new StringTokenizer(line);
          //give the value to the text parameters
          //and change the position of toNode and fromNode to calculate the distance from aim node to query node
          id.set(tokenizer.nextToken());
          toNode.set(tokenizer.nextToken());
          fromNode.set(tokenizer.nextToken());
          distance.set(tokenizer.nextToken());
          context.write(new IntWritable(Integer.parseInt(fromNode.toString())), new Text(toNode + ":" + distance)); 
          }
      }
  
   public static class AdjacencyListReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	   private Text t = new Text();
	   private int queryNodeID = 0;
	   
	   public void setup(Context context) {
		   Configuration config = context.getConfiguration();
		   queryNodeID = config.getInt("queryNodeID", 0);
	   }
	   
	   public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		   StringBuffer sb = new StringBuffer();
		   for (Text val : values) {
			   if(sb.length() == 0){
				   sb = sb.append(val.toString()); //if the sb is empty, sb add the string (val.toString())
				   }
			   else{
				   sb = sb.append(",".concat(val.toString())); //else , add new string with the symbol ","
				   }
			   }
		   String str="Inf"+"\t"; //the string for other from node
		   if (key.get() == queryNodeID) str="0\t"; //the string for aim node(querynode)
           t.set(str + sb.toString()); //combine the str with sb string
           context.write(key, t);
           }
	  }
   
   
   public static class STMapper extends Mapper<Object, Text, LongWritable, Text> {

      @Override
      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
          // YOUR JOB: map function
          // ... ...
    	  Text word = new Text();
    	  String line = value.toString();
    	  String[] sp = line.split("\t"); //split as 0 0 2:1.0,3:2.0 ...
    	  if (sp.length > 2) { //think about the situation 0 0 2:1.0 ...
    		  context.write(new LongWritable(Integer.parseInt(sp[0])), new Text(sp[1] + "\t" + sp[2]));
    	  }
    	  else { //think about the situation 0 1.0 ...
    		  context.write(new LongWritable(Integer.parseInt(sp[0])), new Text(sp[1]));
    	  }
    	  String distanceadd = sp[1]; //set sp[1] as the distance should be added
    	  if (!distanceadd.equals("Inf")) { //calculate if the distanceadd has value.
    		  if (sp.length > 2) { //think about the situation 0 0 2:1.0 ...
	    		  if (sp[2].contains(",")) { //if the sp[2] has more than 1 node and distance,like 1:2.0,2:3.0 ...
		    		  String[] nodes = sp[2].split(","); //split the sp[2] by ","
		    		  for (String node: nodes) { //go through all the nodes
		    			  String[] nodeValue = node.split(":"); // split the node by ":"
		    			  Double distance = Double.parseDouble(distanceadd) + Double.parseDouble(nodeValue[1]); //add the distance should be added and the distance between the key node and value node
		    			  context.write(new LongWritable(Integer.parseInt(nodeValue[0])), new Text(String.valueOf(distance)));
		    		  }
	    		  }
	    		  else { //if the sp[2] has 1 node and distance, like 1:2.0 ...
	    			  if (sp[2].contains(":")) {
	    			      String[] nodeValue = sp[2].split(":"); //split node by ":"
		    			  Double distance = Double.parseDouble(distanceadd) + Double.parseDouble(nodeValue[1]); //add the distance should be added and the distance between the key node and value node
		    			  context.write(new LongWritable(Integer.parseInt(nodeValue[0])), new Text(String.valueOf(distance)));
	    			  }
	    		  }
    		  }
    		  else { //think about the situation 0 2.0 ...
    			  context.write(new LongWritable(Integer.parseInt(sp[0])), new Text(String.valueOf(sp[1])));
    		  }
    	  }
      }
   }
   
   public static class STReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

      @Override
      public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
          // YOUR JOB: reduce function
          // ... ...
    	  Text word = new Text();
    	  String rest = new String();
    	  Double lowest = Double.MAX_VALUE; //set the lowest distance as Double.Max_Value
    	  Double orign = -10.0; //set the original distance as -10.0
    	  Integer judge = 0; //set the whole number of the values
    	  for (Text val : values) {
    		  String vals = val.toString();
    		  String[] nodeValue = vals.split("\t"); //split the value
    		  if (nodeValue.length > 1) { //if the nodeValue.length > 1
    			  rest = nodeValue[1]; //save the rest part of nodes like 1:2.0,3:2.0 ...
    		  }
    		  if (nodeValue[0].equals("0.0")) { //if the node is the queryNode
    			  lowest = 0.0; //set the lowest as 0.0
    			  orign = 0.0; //set the orign as 0.0
    			  judge++; //present key's value number add 1
    		  }
    		  if (nodeValue[0].equals("Inf")) { //if the distance is unknown
    			  judge++; //present key's value number add 1
    		  }
    		  if ((!nodeValue[0].equals("Inf")) && (nodeValue.length > 1)) { //format like 1.0 1:2.0 ...
    			  lowest = Math.min(Double.parseDouble(nodeValue[0]), lowest); //compare with the distance 1.0 and the lowest
    			  orign = Double.parseDouble(nodeValue[0]); //set the original distance as 1.0
    			  judge++; //present key's value number add 1
    		  }
    		  if ((!nodeValue[0].equals("Inf")) && (nodeValue.length == 1)) {  //format like 2 1.0 ...
    			  Double trueDistance = Double.parseDouble(nodeValue[0]); //set the trueDistance as 1.0
    			  lowest = Math.min(lowest, trueDistance); //compare with the trueDistance 1.0 and the lowest
    			  judge++; //present key's value number add 1
    		  }
    	  }
    	  
		  if ((!orign.equals(lowest)) && (judge > 1)) { //if the distance is updated and the present key's value number is more than 1
			  context.getCounter(eInf.COUNTER).increment(1); //counter add 1
		  }
    	  
    	  if (lowest == Double.MAX_VALUE ) { //if the lowest == Double.MAX_VALUE
    		  word.set("Inf\t" + rest); //return the Inf format line
    	  }
    	  else {
    		  word.set(lowest + "\t" + rest); //return the line with the distance
    	  }
    	  context.write(key, word);
    	  word.clear(); //clear the text word
      }
   }

   
   public static class ResultMapper extends Mapper<Object, Text, IntWritable, Text> {

      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
           // YOUR JOB: map function
           // ... ...
    	  Text word = new Text();
          String line = value.toString();
          String[] sp = line.split("\t");
          if (!sp[1].contains("Inf")) { //it means the node can reach the queryNode
	          word.set(sp[1]); //like 2 7.0 ...
	    	  context.write(new IntWritable(Integer.parseInt(sp[0])), word);
          }
       }

   }


   public static class ResultReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	   private int queryNodeID = 0;
	   
	   public void setup(Context context) {
		   Configuration config = context.getConfiguration();
		   queryNodeID = config.getInt("queryNodeID", 0);
	   }

       public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           // YOUR JOB: reduce function
           // ... ...
    	   for (Text val : values) {
    		   Text word = new Text(key + "\t" + val); //combine as 3	7.0 ...
    		   context.write(new IntWritable(queryNodeID),word);
    	   }
    	   
       }
   }

  public static void main(String[] args) throws Exception {        

      IN = args[0];

      OUT = args[1];
      
      int queryNodeID = Integer.parseInt(args[2]);

      String input = IN;

      String output = OUT + System.nanoTime();

	    // YOUR JOB: Convert the input file to the desired format for iteration, i.e., 
      //           create the adjacency list and initialize the distances
      // ... ...
      Configuration conf = new Configuration();
      conf.setInt("queryNodeID", queryNodeID);
      Job job1 = Job.getInstance(conf, "Adjacency List");
      job1.setJarByClass(SingleTargetSP.class);
      job1.setMapperClass(AdjacencyListMapper.class);
      job1.setReducerClass(AdjacencyListReducer.class);
      job1.setOutputKeyClass(IntWritable.class);
      job1.setOutputValueClass(Text.class);
      job1.setNumReduceTasks(1);
      FileInputFormat.addInputPath(job1, new Path(input));
      FileOutputFormat.setOutputPath(job1, new Path(output));
      
      job1.waitForCompletion(true);

      boolean isdone = false;

      while (isdone == false) {

          // YOUR JOB: Configure and run the MapReduce job
          // ... ...     
    	  
          Job job2 = Job.getInstance(conf, "ST Mapper");
          job2.setJarByClass(SingleTargetSP.class);
          job2.setMapperClass(STMapper.class);
          job2.setReducerClass(STReducer.class);
          job2.setOutputKeyClass(LongWritable.class);
          job2.setOutputValueClass(Text.class);
          job2.setNumReduceTasks(1);
          
          input = output;           

          output = OUT + System.nanoTime();

          //You can consider to delete the output folder in the previous iteration to save disk space.

          // YOUR JOB: Check the termination criterion by utilizing the counter
          // ... ...
          FileInputFormat.addInputPath(job2, new Path(input));
          FileOutputFormat.setOutputPath(job2, new Path(output));
          job2.waitForCompletion(true);

          if(job2.getCounters().findCounter(eInf.COUNTER).getValue() == 0){
              isdone = true;
          }
      }

      // YOUR JOB: Extract the final result using another MapReduce job with only 1 reducer, and store the results in HDFS
      // ... ...
      while (isdone == true) {
          Job job3 = Job.getInstance(conf, "Result");
          job3.setJarByClass(SingleTargetSP.class);
          job3.setMapperClass(ResultMapper.class);
          job3.setReducerClass(ResultReducer.class);
          job3.setOutputKeyClass(IntWritable.class);
          job3.setOutputValueClass(Text.class);
          job3.setNumReduceTasks(1);
          
          input = output;

          output = OUT + System.nanoTime();
          
          FileInputFormat.addInputPath(job3, new Path(input));
          FileOutputFormat.setOutputPath(job3, new Path(OUT));
          
          System.exit(job3.waitForCompletion(true) ? 0 : 1);
      }
  }

}

