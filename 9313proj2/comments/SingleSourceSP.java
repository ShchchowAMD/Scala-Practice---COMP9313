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

	public static enum COUNTER{
		CHANGED,M0,
		M1,
		M2,M3,M4,
		GET;
	}
    public static String OUT = "output";
    public static String IN = "input";
    public static String srcMark = "0";
    public static int Vno = 0;
    public static Map<String, Integer> recordsV = new HashMap<String, Integer>();
    public static Map<String, String> path = new HashMap<String, String>();
    
    public static class SSSPMapper extends Mapper<Object, Text, Text, Text> {
	private Text k = new Text();
	private Text v = new Text();
        @Override
       public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(" ");
            if(!recordsV.containsKey(tokens[1])){
            	recordsV.put(tokens[1], 1);
            	Vno++;
            }
            if(!recordsV.containsKey(tokens[2])){
            	recordsV.put(tokens[2], 1);
            	Vno++;
            }
	        k.set(tokens[1]);
	        v.set(tokens[2]+","+tokens[3]);
	        context.write(k, v);
        }

    }


    public static class SSSPReducer extends Reducer<Text, Text, Text, Text> {

	private Text result = new Text();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String v = "";
        	for(Text val: values){
        		if(!v.equals("")) v+=" ";
        		v += val;
        	}
    		if(!v.equals(""))
    			v = " " + v;
        	if(key.toString().equals(srcMark))
        		v = srcMark + ",0.0," + srcMark + v;
        	else
        		v = srcMark + "," + "INF,INF" + v;
        	result.set(v);
            context.write(key, result);
        }
    }


    public static class MidMapper extends Mapper<Object, Text, Text, Text> {
	private Text k = new Text();
	private Text v = new Text();
	private Text k1 = new Text();
	private Text v1 = new Text();
        @Override
       public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // get lines
            String line1 = value.toString();
            String mapKey = line1.split("\t")[0];
            String line = line1.split("\t")[1];
            String[] tokens = line.split(" ");
            String[] dPair = tokens[0].split(","); 
            
            boolean reachable = false;
            k.set(mapKey);
            v.set(line);
            context.write(k, v);
            
            if(dPair[0].equals(srcMark) && !dPair[1].equals("INF"))
            	reachable = true;
            
            if(reachable){
            	// spread the newest distance to its neighbour nodes
            	double currentDis = Double.parseDouble(dPair[1]);
            	for(int i = 1 ; i<tokens.length; i++){
            		String[] t = tokens[i].split(",");
            		double wDis = Double.parseDouble(t[1]) + currentDis;
            		k1.set(t[0]);
            		v1.set("up"+","+mapKey+","+wDis);
            		context.write(k1, v1);
            	}
            }
			
        }

    }


    public static class MidReducer extends Reducer<Text, Text, Text, Text> {
	    private Text outputValue = new Text();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String v = "";
            String originalV = "";
            String fromId = "";
            String originalDis = "";
            String originalFromNode = "";
            boolean updated = false;
            double newDis = 0;
            String[] oArr = null;
        	for(Text val: values){
        		v = val.toString();
        		if(v.startsWith("up")){
        			String[] vArr = v.split(",");
        			fromId = vArr[1];
        			newDis = Double.parseDouble(vArr[2]);
        			updated = true;
        		}
        		else{
        			originalV = val.toString();
        			oArr = originalV.split(" ");
        			originalDis = oArr[0].split(",")[1];
        			originalFromNode = oArr[0].split(",")[2];
        		}
        	}
        	
        	if(originalV.equals("")){
                String result = srcMark + "," + newDis + "," + fromId;
                outputValue.set(result);
            	context.write(key, outputValue);
            	return;
        	}
        	if(originalDis.equals("INF") && updated){
        		originalFromNode = fromId;
        		context.getCounter(COUNTER.CHANGED).increment(1);
        	}
        	if(!originalDis.equals("INF") && updated){
        		double oDis = Double.parseDouble(originalDis);
        		if(oDis<=newDis)
        			newDis = oDis;
        		else{
        			context.getCounter(COUNTER.CHANGED).increment(1);
        			originalFromNode = fromId;
        		}
        	}
            String result = srcMark + ",";
        	if(updated) result += newDis;
        	else result += originalDis;
        	result+=","+originalFromNode;
        	if(oArr.length>1){
        		for(int i=1; i<oArr.length; i++)
        			result += " " + oArr[i];
        	}
			
        	outputValue.set(result);
        	context.write(key, outputValue);
        }
    }


    public static class ExtractMapper extends Mapper<Object, Text, Text, Text> {
	private Text k = new Text();
	private Text v = new Text();
	
        @Override
       public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String result = "";
            String line = value.toString();
            String[] pair = line.split("\t");
            String nodeId = pair[0];
            String nodeInfo = pair[1];
            String[] disVec = nodeInfo.split(" ");
            String shortestDis = disVec[0].split(",")[1];
            String lastNodeId = disVec[0].split(",")[2];
            if(!path.containsKey(nodeId)) path.put(nodeId, lastNodeId);
            result += nodeId+ " " + shortestDis;
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
            
            Map<Integer, String> sortMap = new TreeMap<Integer, String>(
            		new Comparator<Integer>(){
            			public int compare(Integer a, Integer b){
            				return a.compareTo(b);
            			}
            		});
            for(Text val: values){
            	v = "";
            	String[] nodeInfo = val.toString().split(" ");
            	String nodeId = nodeInfo[0];
            	String sPath = nodeInfo[1];
            	
               	String lastNodeId = path.get(nodeId);
                if(sPath.equals("INF")) continue;
                
                ArrayList<String> pathStr = new ArrayList<String>();
                while(true){
                	pathStr.add(new String(lastNodeId));
                	if(lastNodeId.equals(srcMark)) break;
                	lastNodeId = path.get(lastNodeId);
                }
                v += String.format("%.1f", Double.parseDouble(sPath)) + "\t";
                for(int pi=pathStr.size()-1; pi>=0; pi--){
                	if(pi<pathStr.size()-1)
                		v+="->";
                	v += pathStr.get(pi);
                }
            	if(!nodeId.equals(srcMark)){
            		v+="->";
            		v += nodeId;
            	}
                
                sortMap.put(Integer.parseInt(nodeId), v);
            }           
            Iterator<Integer> iter = sortMap.keySet().iterator();
            while(iter.hasNext()){
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

    		if(iteration<Vno-1){
                isdone = midJob.getCounters().findCounter(COUNTER.CHANGED).getValue()==0 ? true:false;
            }
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
