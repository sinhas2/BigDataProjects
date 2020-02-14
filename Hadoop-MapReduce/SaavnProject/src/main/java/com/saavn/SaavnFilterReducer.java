package com.saavn;

import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
 
public class SaavnFilterReducer extends Reducer<Text, Text, Text, Text>{
	
    public void reduce(Text key,  Iterable<Text> values, Context context) throws IOException {
  
    	int total = 0; //to calculate total number of times song streamed
    	
        int total_days = 31; //total number of days
        
        int window = 2; //widow considered to calculate the frequency of song
        
        //variables to hold the calculated count for 7 days
        int window_total_25 = 0;
        int window_total_26 = 0;
        int window_total_27 = 0;
        int window_total_28 = 0;
        int window_total_29 = 0;
        int window_total_30 = 0;
        int window_total_31 = 0;
        
        for(Text val : values) {
        	String[] value = val.toString().split(":");
        	if ((value[0] != null) && (value[1] != null))
        	{
	        	String day = value[0].toString();
	        	String count = value[1].toString();
	        	
	        	/* Calculating the total number of times a particular song is played 
	        	 * in whole month. Adding the weight to the count by day, so that
	        	 * song streamed on latest day weighed more.*/
	        	total = total + (Integer.parseInt(day) * Integer.parseInt(count));
	        	
	        	/* As per defined window, calculating the number of times the particular 
	        	 * song is streamed for each of the seven days (25-31). 
	        	 * Again adding the weight to count by day, so that 
	        	 * latest date can have more weight. */
	        	
	        	//day 25
	        	if ((Integer.parseInt(day) < 25) && (Integer.parseInt(day) >= (25-window)))
	        		window_total_25 = window_total_25 + (Integer.parseInt(day) * Integer.parseInt(count));
	        	
	        	//day 26
	        	if ((Integer.parseInt(day) < 26) && (Integer.parseInt(day) >= (26-window)))
	        		window_total_26 = window_total_26 + (Integer.parseInt(day) * Integer.parseInt(count));
	        	
	        	//day 27
	        	if ((Integer.parseInt(day) < 27) && (Integer.parseInt(day) >= (27-window)))
	        		window_total_27 = window_total_27 + (Integer.parseInt(day) * Integer.parseInt(count));
	        	
	        	//day 28
	        	if ((Integer.parseInt(day) < 28) && (Integer.parseInt(day) >= (28-window)))
	        		window_total_28 = window_total_28 + (Integer.parseInt(day) * Integer.parseInt(count));
	        	
	        	//day 29
	        	if ((Integer.parseInt(day) < 29) && (Integer.parseInt(day) >= (29-window)))
	        		window_total_29 = window_total_29 + (Integer.parseInt(day) * Integer.parseInt(count));
	        	
	        	//day 30
	        	if ((Integer.parseInt(day) < 30) && (Integer.parseInt(day) >= (30-window)))
	        		window_total_30 = window_total_30 + (Integer.parseInt(day) * Integer.parseInt(count));
	        	
	        	//day 31
	        	if ((Integer.parseInt(day) < 31) && (Integer.parseInt(day) >= (31-window)))
	        		window_total_31 = window_total_31 + (Integer.parseInt(day) * Integer.parseInt(count));
	        	
        	}
        }
        
        /* Trending songs are calculated on approach to find out the difference between
         * song played in the window compared to the average. If difference is huge, it shows
         * the positive growth rate. Hence, the results will be sorted on the frequency calculated
         * and picking up the top 100 from the result.
         * */
        int avg_total = total/total_days;
        int freq_25 = window_total_25 - avg_total;
        int freq_26 = window_total_26 - avg_total;
        int freq_27 = window_total_27 - avg_total;
        int freq_28 = window_total_28 - avg_total;
        int freq_29 = window_total_29 - avg_total;
        int freq_30 = window_total_30 - avg_total;
        int freq_31 = window_total_31 - avg_total;
        
        try {
        	//writing songid along with frequency calculated for 7 days
			context.write(key, new Text(freq_25 + "," + freq_26 + "," + freq_27 + "," +
					freq_28 + "," + freq_29 + "," + freq_30 + "," + freq_31));
		} catch (InterruptedException e) {
		
			e.printStackTrace();
		}     
	
    }
}