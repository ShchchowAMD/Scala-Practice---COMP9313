import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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


public class WordAvgLen1 {
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

          // map method, send <K,V> to combiner
          // input: <Obj, Text>
          // output: <Text, countPair>
		  public static class TokenizerMapper
		       extends Mapper<Object, Text, Text, countPair>{

		    private Text word = new Text();

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
                      
                      // new countPair, will be sent to combiner
                      // 1 means the number of this word is 1
                      // tmpStr.length means the length of the word
			    	  countPair tmpVal = new countPair(1, tmpStr.length());
                      
                      // Key = the first character of the word
			    	  word.set(tmpStr.charAt(0)+"");
			    	  context.write(word, tmpVal);
		    	  }
		      }
		    }
		  }

          // input: from map, <K1, V1> (<Text, countPair>)
          // output: to reducer, <K2, V2> (<Text, countPair>, the same format)
		  public static class Combiner extends Reducer<Text, countPair, Text, countPair>{

			    private countPair result = new countPair();
			    
			    public void reduce(Text key, Iterable<countPair> values,
			                       Context context
			                       ) throws IOException, InterruptedException {
			      
                  // calculate the total number and length of a Key (a-z), and send it to reducer
                  int No = 0;
			      int lenthTotal = 0;
			      for (countPair val : values) {
			        No += val.getNo();
				    lenthTotal += val.getLen();
			      }
                  
                  // send to reducer
			      result.set(No, lenthTotal);
			      context.write(key, result);
			    }
		  }
		  public static class countReducer
		       extends Reducer<Text,countPair,Text,DoubleWritable> {
		    
		    private DoubleWritable result = new DoubleWritable();

		    public void reduce(Text key, Iterable<countPair> values,
		                       Context context
		                       ) throws IOException, InterruptedException {
              // calculate the avg length in reducer
		      double sum = 0;
		      int iteNo = 0;
              
              // actually, only 1 instance here
		      for (countPair val : values) {
		        sum += val.getLen();
		        iteNo += val.getNo();
		      }
              
              //output
		      result.set(sum/iteNo);
		      context.write(key, result);
		    }
		  }

		  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "word count");
		    job.setJarByClass(WordAvgLen1.class);
		    job.setMapperClass(TokenizerMapper.class);
            // set combiner
		    job.setCombinerClass(Combiner.class);
		    job.setReducerClass(countReducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(countPair.class);
		    
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}
