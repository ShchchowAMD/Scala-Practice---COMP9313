package comp9313.ass2;
import java.io.*;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.hash.Hash;

public class SingleSourceSP {
    // counter and testing status/enums
	public static enum COUNTER{
		CHANGED,M0,
		M1,
		M2,M3,M4,
		GET;
	}    // default output path
    public static String OUT = "output";    // default input path
    public static String IN = "input";    // default source point's name
    public static String srcMark = "0";    // using Bellman-Ford algorithm, so need to store the number of points
    public static int Vno = 0;
    public static Map<String, Integer> recordsV = new HashMap<String, Integer>();    // tmp map, to get path
    public static Map<String, String> path = new HashMap<String, String>();
        // first mapper and reducer, to seperate lines, store in format:    // [nodeId sourceId,distanceToSource,lastNodeId {dstNodeId,distance}]
    public static class SSSPMapper extends Mapper<Object, Text, Text, Text> {    // output value
	private Text k = new Text();
	private Text v = new Text();
        @Override
       public void map(Object key, Text value, Context context) throws IOException, InterruptedException {            // get lines in input graph file
            String line = value.toString();
            String[] tokens = line.split(" ");            // records nodes and store the number of nodes
            if(!recordsV.containsKey(tokens[1])){
            	recordsV.put(tokens[1], 1);
            	Vno++;
            }
            if(!recordsV.containsKey(tokens[2])){
            	recordsV.put(tokens[2], 1);
            	Vno++;
            }            // pass <K,V> to reducer in format            // <nodeId, "dstNodeId, distance">
	        k.set(tokens[1]);
	        v.set(tokens[2]+","+tokens[3]);
	        context.write(k, v);
        }

    }


    public static class SSSPReducer extends Reducer<Text, Text, Text, Text> {

	private Text result = new Text();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String v = "";            // get value , seperate all {dstNodeId,distance} with " "
        	for(Text val: values){
        		if(!v.equals("")) v+=" ";
        		v += val;
        	}
    		if(!v.equals(""))
    			v = " " + v;            // add source mark , its current distance and last node in path from source node
        	if(key.toString().equals(srcMark))                // if it is source node
        		v = srcMark + ",0.0," + srcMark + v;
        	else                // else, set distance to Infinity and last node also Infinity
        		v = srcMark + "," + "INF,INF" + v;
        	result.set(v);
            context.write(key, result);
        }
    }

    // second mapper reducer.    // based on Bellman-Ford     // spread current distance from each node to all nodes they connected with.    // if Dis(u) + w(u,k) < Dis(k)    // then Dis(u) + w(u,k) -> Dis(k)    // this will repeat for no more than V-1 times, where V-1 is the number of nodes    // and for each iteration, when there's no changes in all distances, end the iteration.
    public static class MidMapper extends Mapper<Object, Text, Text, Text> {    // output paras
	private Text k = new Text();
	private Text v = new Text();
	private Text k1 = new Text();
	private Text v1 = new Text();
        @Override
       public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // get lines
            String line1 = value.toString();            // file format:  [nodeId sourceId,distanceToSource,lastNodeId {dstNodeId,distance}]                        // node id
            String mapKey = line1.split("\t")[0];
            String line = line1.split("\t")[1];            // [sourceId,distanceToSource,lastNodeId {dstNodeId,distance}]
            String[] tokens = line.split(" ");            // [sourceId,distanceToSource,lastNodeId]
            String[] dPair = tokens[0].split(","); 
                        // if this node's distance is not infinity
            boolean reachable = false;                        // output original data for next iteration
            k.set(mapKey);
            v.set(line);
            context.write(k, v);
                        // if the distance is not infinity
            if(dPair[0].equals(srcMark) && !dPair[1].equals("INF"))
            	reachable = true;
            
            if(reachable){
            	// spread the newest distance to its neighbour nodes
            	double currentDis = Double.parseDouble(dPair[1]);
            	for(int i = 1 ; i<tokens.length; i++){                    
            		String[] t = tokens[i].split(",");                    // Dis(u) + w(u,k)                    // new distance for k, if it choose to reach source node by u
            		double wDis = Double.parseDouble(t[1]) + currentDis;                    // spread the new distance to node u.
            		k1.set(t[0]);                    // ['up', fromNodeId, newDistance]
            		v1.set("up"+","+mapKey+","+wDis);
            		context.write(k1, v1);
            	}
            }
			
        }

    }

    // update the distance of node K to source node if Dis(u) + w(u,k) < Dis(k)
    public static class MidReducer extends Reducer<Text, Text, Text, Text> {
	    private Text outputValue = new Text();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {            // tmp result string
            String v = "";            // original data in last mapreduce result
            String originalV = "";                        // point u's node id
            String fromId = "";                        // node's current distance to source node
            String originalDis = "";            // node's current LastNode in the path to source node
            String originalFromNode = "";            // whether the node receive a "up" msg from its neighbour
            boolean updated = false;            // the result of Dis(u) + w(u,k) < Dis(k) ? Dis(u) + w(u,k) : Dis(k)
            double newDis = 0;            // original data array
            String[] oArr = null;
        	for(Text val: values){
        		v = val.toString();                // receive new distance from neighbour
        		if(v.startsWith("up")){
        			String[] vArr = v.split(",");                    // record neighbour node's id
        			fromId = vArr[1];                    // record new distance Dis(u) + w(u,k)
        			newDis = Double.parseDouble(vArr[2]);
        			updated = true;
        		}
        		else{
        			originalV = val.toString();
        			oArr = originalV.split(" ");                    // original distance, could be INF
        			originalDis = oArr[0].split(",")[1];                    // original "lastNode", could be INF, which stands for not initial
        			originalFromNode = oArr[0].split(",")[2];
        		}
        	}
        	            // if this node never becomes starting point in an edge            // it should also be registered
        	if(originalV.equals("")){
                String result = srcMark + "," + newDis + "," + fromId;
                outputValue.set(result);
            	context.write(key, outputValue);
            	return;
        	}            // if Dis(k) == INF, then update Dis(k) = Dis(u) + w(u,k)
        	if(originalDis.equals("INF") && updated){
        		originalFromNode = fromId;
        		context.getCounter(COUNTER.CHANGED).increment(1);
        	}            // if Dis(k) != INF, then update Dis(k) = Dis(u) + w(u,k) < Dis(k) ? Dis(u) + w(u,k) : Dis(k)
        	if(!originalDis.equals("INF") && updated){
        		double oDis = Double.parseDouble(originalDis);
        		if(oDis<=newDis)                    // original distance is shorter
        			newDis = oDis;
        		else{                // new distance is shorter                // using COUNTER to record a change
        			context.getCounter(COUNTER.CHANGED).increment(1);                    // update fromNode id
        			originalFromNode = fromId;
        		}
        	}            // update distance in each iteration
            String result = srcMark + ",";
        	if(updated) result += newDis;
        	else result += originalDis;
        	result+=","+originalFromNode;            // rewrite same edges information
        	if(oArr.length>1){
        		for(int i=1; i<oArr.length; i++)
        			result += " " + oArr[i];
        	}
			
        	outputValue.set(result);
        	context.write(key, outputValue);
        }
    }

    // get final result and path.
    public static class ExtractMapper extends Mapper<Object, Text, Text, Text> {
	private Text k = new Text();
	private Text v = new Text();
	
        @Override
       public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String result = "";
            String line = value.toString();            // file format:  [nodeId\tsourceId,distanceToSource,lastNodeId {dstNodeId,distance}]
            String[] pair = line.split("\t");            // node id
            String nodeId = pair[0];            // other info, see file format
            String nodeInfo = pair[1];
            String[] disVec = nodeInfo.split(" ");            // shortest path to source node
            String shortestDis = disVec[0].split(",")[1];            // last node in the path
            String lastNodeId = disVec[0].split(",")[2];            // store its last node
            if(!path.containsKey(nodeId)) path.put(nodeId, lastNodeId);            // output format: [nodeId shortestPathToSource]
            result += nodeId+ " " + shortestDis;            // single reducer in extrator
            k.set("1");
            v.set(result);
            context.write(k, v);
        }

    }


    public static class ExtractReducer extends Reducer<Text, Text, Text, Text> {

	private Text result = new Text();
	private Text k = new Text();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String v = "";
                        // sort the result based on specification
            Map<Integer, String> sortMap = new TreeMap<Integer, String>(
            		new Comparator<Integer>(){
            			public int compare(Integer a, Integer b){
            				return a.compareTo(b);
            			}
            		});
            for(Text val: values){
            	v = "";
            	String[] nodeInfo = val.toString().split(" ");
            	String nodeId = nodeInfo[0];                                // shortest path for this node. could be INF
            	String sPath = nodeInfo[1];
            	
               	String lastNodeId = path.get(nodeId);                // if this node couldn't reach query node, dont record it
                if(sPath.equals("INF")) continue;
                
                ArrayList<String> pathStr = new ArrayList<String>();
                while(true){                    // back tracking
                	pathStr.add(new String(lastNodeId));                    // find the path to source node
                	if(lastNodeId.equals(srcMark)) break;                    // back tracking
                	lastNodeId = path.get(lastNodeId);
                }                // same as sample output format
                v += String.format("%.1f", Double.parseDouble(sPath)) + "\t";                // get path based on back tracking
                for(int pi=pathStr.size()-1; pi>=0; pi--){
                	if(pi<pathStr.size()-1)
                		v+="->";
                	v += pathStr.get(pi);
                }                // source node's path is only "nodeId"
            	if(!nodeId.equals(srcMark)){
            		v+="->";
            		v += nodeId;
            	}
                                // put <K,V> in sort tree
                sortMap.put(Integer.parseInt(nodeId), v);
            }           
            Iterator<Integer> iter = sortMap.keySet().iterator();
            while(iter.hasNext()){            // write the result to final result file. order based on its numeric value
            	Integer keyIte = iter.next();
	        	result.set(sortMap.get(keyIte));
	        	k.set(""+keyIte);
	            context.write(k, result);
            }
        }
    }
    public static void main(String[] args) throws Exception {        

        IN = args[0];

        OUT = args[1];
        
        if(args.length>2)
        	srcMark = args[2];

        int iteration = 0;

        String input = IN;

        String output = OUT + iteration ;

        boolean isdone = false;

    	Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "SingleSourceSP");
	
		job.setJarByClass(SingleSourceSP.class);
		job.setMapperClass(SSSPMapper.class);
		job.setReducerClass(SSSPReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(false);

        while (isdone == false) {
        	input = output;
    		Job midJob = Job.getInstance(conf, "iteration");
    	
    		midJob.setJarByClass(SingleSourceSP.class);
    		midJob.setMapperClass(MidMapper.class);
    		midJob.setReducerClass(MidReducer.class);
    		midJob.setOutputKeyClass(Text.class);
    		midJob.setOutputValueClass(Text.class);

    		FileInputFormat.addInputPath(midJob, new Path(input));
            iteration ++;   
        	output = OUT + iteration;
    		FileOutputFormat.setOutputPath(midJob, new Path(output));
    		midJob.waitForCompletion(false);
            // if no distance change happens, stop
    		if(iteration<Vno-1){
                isdone = midJob.getCounters().findCounter(COUNTER.CHANGED).getValue()==0 ? true:false;
            }            // or after V-1 times, stop
            if(iteration>=Vno-1){
            	isdone = true;System.out.println("2");
            }
        }

        input = input;
        iteration ++;   
    	output = OUT;
    	
		Job extractJob = Job.getInstance(conf, "Extract");
		extractJob.setJarByClass(SingleSourceSP.class);
		extractJob.setMapperClass(ExtractMapper.class);
		extractJob.setReducerClass(ExtractReducer.class);
		extractJob.setNumReduceTasks(1);
		extractJob.setOutputKeyClass(Text.class);
		extractJob.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(extractJob, new Path(input));
		FileOutputFormat.setOutputPath(extractJob, new Path(output));
		System.exit(extractJob.waitForCompletion(false)? 0:1);

    }

}

