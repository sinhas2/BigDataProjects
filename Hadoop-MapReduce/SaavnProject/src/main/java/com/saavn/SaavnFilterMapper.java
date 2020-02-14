package com.saavn;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SaavnFilterMapper extends
Mapper<LongWritable, Text, Text, Text> {
	@Override

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		//stream = song ID, user ID, timestamp, hour, date 
		String st [] = value.toString().split(",");
		String songId = st[0];
		String[] date = st[4].toString().split("-");
		if (date.length == 3) {
			String day = date[2]; 
			String count = "1";
	
			//Adding song ID along with day and count
			if ((songId != null) && (day != null))
				context.write(new Text(songId), new Text(day + ":" + count));
		}
	}
}

