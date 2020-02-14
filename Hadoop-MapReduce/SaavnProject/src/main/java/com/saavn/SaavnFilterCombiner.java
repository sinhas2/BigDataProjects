package com.saavn;

import java.io.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
 
public class SaavnFilterCombiner extends Reducer<Text, Text, Text, Text>{
	
    public void reduce(Text key,  Iterable<Text> values, Context context) throws IOException {
    	
    	//combiner to only calculate the count
        int ctotal = 0;
        String day = "";
    	 for(Text val : values) {
         	String[] value = val.toString().split(":");
         	if ((value[0] != null) && (value[1] != null)) {
	         	day = value[0].toString();
	         	String count = value[1].toString();
	         	
	         	//to sum up the count
	         	ctotal = ctotal + Integer.parseInt(count);
         	}
         	
         }
        
        try {
        	if (ctotal != 0)
        		context.write(key, new Text(day + ":" + Integer.toString(ctotal)));
		} catch (InterruptedException e) {
		
			e.printStackTrace();
		}     
	
    }
}

