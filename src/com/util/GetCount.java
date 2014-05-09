package com.util;

import java.sql.Timestamp;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Mapper class to output <key,value> pairs of <year+month, 1> to count number of events in each year each month.
 * @author nikhilrane
 *
 */
public class GetCount extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> 
{
	private Text outKey = new Text();
	private IntWritable one = new IntWritable(1);
	

	//id|title|start_time|stop_time|venue_name|latitude|longitude|description|category|recur_string|created|modified|owner|url|tags|venue_id|gotvenue
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) 
	{
		try
		{
			
			String line;
			
			line = value.toString();		//got one event information
			StringTokenizer eventTokens = new StringTokenizer(line, "|");
			
			//initialize our EventInfo object
			if(eventTokens.hasMoreTokens())
			{
				//ACTUAL: id|title|start_time|stop_time|venue_name|latitude|longitude|description|category|recur_string|created|modified|owner|url|tags|venue_id|gotvenue
				//USED:   id|title|start_time|stop_time|venue_name|latitude|longitude|description|category|recur_string|url|tags|venue_id
				eventTokens.nextToken();
				eventTokens.nextToken();
				
				String startTime = eventTokens.nextToken();
				if(startTime.contains("+"))
					startTime = startTime.substring(0, startTime.length()-3);
				
				Timestamp startTS = Timestamp.valueOf(startTime);
				@SuppressWarnings("deprecation")
				int year = startTS.getYear();
				@SuppressWarnings("deprecation")
				int month  = startTS.getMonth();

				if(year == 111 || year == 112)
				{
					outKey.set(Integer.toString(year) + "|" + Integer.toString(month));
					output.collect(outKey, one);
				}
			}
			
			
				
			
		} catch(Exception e)
		{
			System.out.println("Caught Exception: "+e);
//			e.printStackTrace();
		}
		
		
	}
}
