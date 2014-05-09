package com.planner;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * Outputs count of tag occurrence for this venue to find trend.
 * @author nikhilrane
 *
 */
public class FindTrendReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> 
{
	
	/**
	 * Counts number of occurrences of specific tag for specific venue and outputs final count as key value pair of <venue+tag, count>
	 */
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException 
	{
		int sum = 0;
		
		while (values.hasNext()) 
		{
			sum += values.next().get();
		}
		output.collect(key, new IntWritable(sum));
	}
}

