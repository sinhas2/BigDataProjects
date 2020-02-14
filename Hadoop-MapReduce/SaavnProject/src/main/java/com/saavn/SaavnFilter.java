package com.saavn;

import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
public class SaavnFilter extends Configured implements Tool{
 
	
	public static void main(String[] args) throws Exception {
	        int returnStatus = ToolRunner.run(new Configuration(), new SaavnFilter(), args);
	        System.exit(returnStatus);
	    }

    
	public int run(String[] args) throws IOException{
   
  
  
    	Job job = new Job(getConf());
    	
    	
    	 job.setJobName("Saavn Filter");
    	
    	 job.setJarByClass(SaavnFilter.class);

    	 job.setOutputKeyClass(Text.class);
    	 job.setOutputValueClass(Text.class);
    	 
    	 job.setMapperClass(SaavnFilterMapper.class);
    	 //combiner is required to reduce disk I/O
    	 job.setCombinerClass(SaavnFilterCombiner.class);
    	 job.setReducerClass(SaavnFilterReducer.class);
    	 
    	 /*Partitioner is not required as approach calculates the frequency
    	 as per songid. By default, partitioner will take the songid and
    	 send it to the same reducer.
    	 [return (key.hashCode() & Integer.MAX_VALUE) %numReduceTasks;]
    	 */
    	 //job.setPartitionerClass(SaavnFilterPartitioner.class);
    	 job.setNumReduceTasks(10);
    	
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job,new Path(args[1]));
    	   	
    	
    	try {
			return job.waitForCompletion(true) ? 0 : 1;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
    	
    	
      
    }
 
   
 
}