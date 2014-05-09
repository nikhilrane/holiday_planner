package com.planner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Processes 'tags' field to find 'trend' at candidate venues (which are found in FindCandidateEvents job). 
 * @author nikhilrane
 *
 */
public class FindTrendMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> 
{
	private final static IntWritable one = new IntWritable(1);
	private Text venueID = new Text();
	private Path localPath[];
	private static ArrayList<String> venueData = new ArrayList<String>();
	
	/**
	 * Gets our cached files.
	 */
	public void configure(JobConf job) {
        try
        {
        	localPath = DistributedCache.getLocalCacheFiles(job);
        } catch(Exception e)
        {
        	System.out.println("Exception in getting DS Cache files.."+e);
        	e.printStackTrace();
        }
        
      }
	
	

	//id|title|start_time|stop_time|venue_name|latitude|longitude|description|category|recur_string|created|modified|owner|url|tags|venue_id|gotvenue
	/**
	 * Outputs <key,value> pairs as <venue_ID+tag, 1> for counting occurrence of a specific tag in an event at a specific venue.
	 */
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) 
	{
		try
		{
			
			String line = value.toString();		//got one event information with first field as: Venue_ID
			
			BufferedReader inCandidateFile = new BufferedReader(new FileReader(localPath[1].toString()));
			while((line = inCandidateFile.readLine()) != null)					//create list of all venue_IDs so we do not make this as a bottleneck to read files again & again
			{
				//V0-001-003699774-0|E0-001-036995779-6|...|2011-03-05 20:00:00|2011-03-05 20:00:00|...|39.491188|-84.32839|" "|music|" "|...|concert,music,reverbnationcom|V0-001-003699774-0
				StringTokenizer eventTokens = new StringTokenizer(line, "|");
				
				if(eventTokens.hasMoreTokens())
				{
					String venue_id = eventTokens.nextToken();
					venueData.add(venue_id);
				}
			}
			inCandidateFile.close();		//file reading is over so close file reader object!
			
			
			line = value.toString();
			StringTokenizer eventTokens = new StringTokenizer(line, "|");
			if(eventTokens.hasMoreTokens())			//find tags for this venue
			{
				//ACTUAL: id|title|start_time|stop_time|venue_name|latitude|longitude|description|category|recur_string|created|modified|owner|url|tags|venue_id|gotvenue
				//USED:   id|title|start_time|stop_time|venue_name|latitude|longitude|description|category|recur_string|url|tags|venue_id
				for(int i=0; i < 14; i++)		//skip fields till 'tags' field arrives
					eventTokens.nextToken();

				String tags = eventTokens.nextToken();
				String venue_id = eventTokens.nextToken();
				
				if(venueData.contains(venue_id) && (tags != null && tags.length() > 1) )		//if this is a venue from candidate events list
				{
					StringTokenizer tagTokens = new StringTokenizer(tags, ",");
					
					while(tagTokens.hasMoreElements())											//process the tags to find trend
					{
						venueID.set(venue_id + "|" +tagTokens.nextToken().trim());
						output.collect(venueID, one);
					}
				}
			}
			
			
		} catch(Exception e)
		{
			System.out.println("Caught Exception: "+e);
			e.printStackTrace();
		}
		
		
	}
}
