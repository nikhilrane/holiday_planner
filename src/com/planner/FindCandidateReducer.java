package com.planner;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.vob.EventInfo;

/**
 * Reducer to just output <key,value> pairs.
 * @author nikhilrane
 *
 */
public class FindCandidateReducer extends MapReduceBase implements Reducer<Text, EventInfo, Text, EventInfo> 
{
	
	/**
	 * Output <key,value> pair as <venue_ID,eventInfo>.
	 */
	public void reduce(Text key, Iterator<EventInfo> values, OutputCollector<Text, EventInfo> output, Reporter reporter) throws IOException 
	{
		while (values.hasNext()) 
		{
			EventInfo e = values.next();
			output.collect(key, e);
		}
	}
}

