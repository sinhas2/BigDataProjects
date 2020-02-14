package com.saavn;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public class SaavnFilterPartitioner extends Partitioner<Text,Text > implements
    Configurable {

  private Configuration configuration;

  /**
   * Set up the months hash map in the setConf method.
   */
  public void setConf(Configuration configuration) {
    this.configuration = configuration;
  }

  /**
   * Implement the getConf method for the Configurable interface.
   */
  public Configuration getConf() {
    return configuration;
  }

  /**
   * You must implement the getPartition method for a partitioner class.
   * This method receives the three-letter abbreviation for the month
   * as its value. (It is the output value from the mapper.)
   * It should return an integer representation of the month.
   * Note that January is represented as 0 rather than 1.
   * 
   * For this partitioner to work, the job configuration must have been
   * set so that there are exactly 12 reducers.
   */
  public int getPartition(Text key, Text value, int numReduceTasks) {
	  return (key.hashCode() & Integer.MAX_VALUE) %numReduceTasks;
  }
}

