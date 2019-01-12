import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordAvgLen2 {
    // in-class structure, store the number of wards begin with specific char and its length
	  public static class countPair implements Writable{
            // the number of wards begin with specific char and its length

			private int no, len;

			public countPair(){}
			
			public countPair(int inputNo, int inputLen){
				no = inputNo;
				len = inputLen;
			}
			
            // Override, implements Writable in hadoop pkg
			public void set(int inputNo, int inputLen){
				this.no = inputNo;
				this.len = inputLen;
			}

			public int getNo(){ return no;}
			public int getLen(){ return len;}
			@Override
			public void readFields(DataInput arg0) throws IOException {
				// TODO Auto-generated method stub
				no = arg0.readInt();
				len = arg0.readInt();
			}

			@Override
			public void write(DataOutput arg0) throws IOException {
				// TODO Auto-generated method stub
				arg0.writeInt(no);
				arg0.writeInt(len);
			}
			  
		  }

          // map method, send <K,V> to reducer
          // in-mapping combiner
          // method cleanup will be automatically called before sending data to reducer
          // input: <Obj, Text>
          // output: <Text, countPair>
		  public static class TokenizerMapper
		       extends Mapper<Object, Text, Text, countPair>{

		    private Text word = new Text();

            // use hash map to store the: First character of the word(Ch), Total Number(No), Total Length(Len)
            // the structure is like: <Ch,<No,Len>>
		    Map<Character, Entry<Integer, Integer>> hMap = new HashMap<Character, Entry<Integer, Integer>>();
		    
		    public void map(Object key, Text value, Context context
		                    ) throws IOException, InterruptedException {
              // seperate articles into words
		      StringTokenizer itr = new StringTokenizer(value.toString(),
		              " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");
		      while (itr.hasMoreTokens()) {
		    	  
		    	  // all upper case will be changed to lower case: eg,'A'->'a'
		    	  String tmpStr  =  itr.nextToken().toLowerCase();
		    	  
                  //filter, only keep words begin with 'a-z'
		    	  if(tmpStr.charAt(0) >= 'a' && tmpStr.charAt(0) <= 'z' ){
		    		  
                      // first time find words begin with Ch, store it in hash map
		    		  if(!hMap.containsKey(tmpStr.charAt(0)))
		    			  hMap.put(tmpStr.charAt(0), new AbstractMap.SimpleEntry<Integer, Integer>(1, tmpStr.length()));
		    		  else
		    		  {
                          // already have records which key = ch
                          // then, the total number ++
                          // then, sum and update the total length
                          
                          // previous total length
		    			  int previousLength = hMap.get(tmpStr.charAt(0)).getValue().intValue();
                          
                          // current total number 
		    			  int currNO = hMap.get(tmpStr.charAt(0)).getKey().intValue();
		    			  currNO ++ ;
                          
                          //current total length
		    			  int currLength = previousLength + tmpStr.length();
                          
                          //update
		    			  hMap.put(tmpStr.charAt(0), new AbstractMap.SimpleEntry<Integer, Integer>(currNO, currLength));
		    		  }
		    	  }
		      }
		    }
		    
            // combine
		    public void cleanup(Context context) throws IOException, InterruptedException{
		    	  Iterator<Entry<Character, Entry<Integer, Integer> > > ite = hMap.entrySet().iterator();
		    	  while(ite.hasNext()){
		    		  Entry<Character, Entry<Integer, Integer> > entry = ite.next();
		    		  
                      // send all records to reducer
		    		  String ch = entry.getKey()+"";
		    		  int chNo = entry.getValue().getKey().intValue();
		    		  int chLength = entry.getValue().getValue().intValue();
		    		  context.write(new Text(ch), new countPair(chNo, chLength));
		    	  }
		    }
		  }

          // input: from map, <K1, V1> (<Text, countPair>)
		  public static class countReducer
		       extends Reducer<Text,countPair,Text,DoubleWritable> {
		    
		    private DoubleWritable result = new DoubleWritable();

		    public void reduce(Text key, Iterable<countPair> values,
		                       Context context
		                       ) throws IOException, InterruptedException {
                                   
              // calculate the avg length in reducer
		      double sum = 0;
		      int iteNo = 0;
		      for (countPair val : values) {
		        sum += val.getLen();
		        iteNo += val.getNo();
		      }
		      result.set(sum/iteNo);
		      context.write(key, result);
		    }
		  }

		  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "word count");
		    job.setJarByClass(WordAvgLen2.class);
            // now its in-mapping combiner
		    job.setMapperClass(TokenizerMapper.class);
		    job.setReducerClass(countReducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(countPair.class);
		    
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}
