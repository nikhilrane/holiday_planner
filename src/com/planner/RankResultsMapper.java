package com.planner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.vob.EventInfo;
import com.vob.Person;

/**
 * Mapper class to rank events based on trend for System and likes for each person.
 * @author nikhilrane
 *
 */
public class RankResultsMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, EventInfo> 
{
	//constants for our output keys
	private static final String SYSTEM_TOP = "system|top";
	private static final String SYSTEM_LOW = "system|low";
	private static final String USER_TOP = "|top";
	private static final String USER_LOW = "|low";
	
	private Text outputKey = new Text();
	private Path localPath[];
	private static ArrayList<Person> personData = new ArrayList<Person>();
	private HashMap<String, String> topTags = new HashMap<String, String>();
	
	/**
	 * Gets our cached files.
	 */
	public void configure(JobConf job) {
        try
        {
        	localPath = DistributedCache.getLocalCacheFiles(job);		//1. personData, 2. top tags
        } catch(Exception e)
        {
        	System.out.println("Exception in getting DS Cache files.."+e);
        	e.printStackTrace();
        }
        
      }
	

	/**
	 * Checks if current event's tags contain the 'top tag' for event's venue to generate System recommended rankings. For each person, does the same using person's 'likes' field. 
	 */
	public void map(LongWritable key, Text value, OutputCollector<Text, EventInfo> output, Reporter reporter) 
	{
		try
		{
			
			String line;
			EventInfo event = new EventInfo();
			
			if(personData.isEmpty())				//if our person's list is empty, generate it to avoid bottleneck of reading everytime.
			{
				//let's get our person data
				BufferedReader inPersonData = new BufferedReader(new FileReader(localPath[0].toString()));
				while((line = inPersonData.readLine()) != null)
				{
					StringTokenizer personTokens = new StringTokenizer(line, "|");
					Person p;
					if(personTokens.hasMoreTokens())
					{
						p = new Person();
						
						//Name|latitude|longitude|startTime|endTime|radius|likes
						p.setName(personTokens.nextToken());
						p.setLatitude(Double.parseDouble(personTokens.nextToken()));
						p.setLongitude(Double.parseDouble(personTokens.nextToken()));
						
						String startTime = personTokens.nextToken();
						if(startTime.contains("+"))		startTime = startTime.substring(0, startTime.length()-3);
						p.setStartTime(Timestamp.valueOf(startTime));
						
						String endTime = personTokens.nextToken();
						if(endTime.contains("+"))		endTime = endTime.substring(0, endTime.length()-3);
						p.setEndTime(Timestamp.valueOf(endTime));
						
						
						p.setRadius(Long.parseLong(personTokens.nextToken()));
						p.setLikes(personTokens.nextToken());
						personData.add(p);							//add person's data to our list
					}
				}
				inPersonData.close();		//file reading is over so close file reader object!
			}
			
			
			
			//generate a list <venue_ID, top_tag> using output of FindTrend job.
			BufferedReader inTopTags = new BufferedReader(new FileReader(localPath[1].toString()));
			String currentVenue = null;
			String topTag = null;
			int maxValue = 0;
			
			while((line = inTopTags.readLine()) != null)			//find top tag for this venue and add it to list
			{
				StringTokenizer tagTokens = new StringTokenizer(line, "|");
				
				if(tagTokens.hasMoreElements())
				{
					String venue = tagTokens.nextToken();						//venue in current line
					
					if(currentVenue == null)
					{
						currentVenue = venue;
						topTag = null;
						maxValue = 0;
					}
					
					if((currentVenue != null && !currentVenue.equals(venue)))
					{
						topTags.put(currentVenue, topTag);					//add our processed venue<=>topTag value to HashMap
						currentVenue = venue;
						topTag = null;
						maxValue = 0;
					}
					
					String currentTag = tagTokens.nextToken();
					int currentValue = Integer.parseInt(tagTokens.nextToken());
					if( (topTag == null) || (maxValue < currentValue) )
					{
						topTag = currentTag;
						maxValue = currentValue;
					}
				}
			}
			
			topTags.put(currentVenue, topTag);					//add our last processed venue<=>topTag value to HashMap
			inTopTags.close();									//file reading is over so close file reader object!
			
			
			
			line = value.toString();		//got one event information
			StringTokenizer eventTokens = new StringTokenizer(line, "|");
			
			//initialize our EventInfo object
			if(eventTokens.hasMoreTokens())
			{
				//venue_id|id|title|start_time|stop_time|venue_name|latitude|longitude|description|category|recur_string|url|tags|venue_id
				eventTokens.nextToken();
				event.setId(eventTokens.nextToken());
				event.setTitle(eventTokens.nextToken());
				
				String startTime = eventTokens.nextToken();
				if(startTime.contains("+"))
					startTime = startTime.substring(0, startTime.length()-3);
				
				
				event.setStart_time(startTime);
				
				String stopTime = eventTokens.nextToken();
				if(stopTime.contains("+"))
					stopTime = stopTime.substring(0, stopTime.length()-3);
				
				if(stopTime == null || stopTime.length() < 8)
					event.setStop_time(startTime);
				else
					event.setStop_time((stopTime));
				
				event.setVenue_name(eventTokens.nextToken());
				
				event.setLatitude((Double.parseDouble(eventTokens.nextToken())));
				event.setLongitude((Double.parseDouble(eventTokens.nextToken())));
				
				event.setDescription(eventTokens.nextToken());
				event.setCategory(eventTokens.nextToken());
				event.setRecur_string(eventTokens.nextToken());
				
				event.setUrl(eventTokens.nextToken());
				event.setTags(eventTokens.nextToken());
				event.setVenue_id(eventTokens.nextToken());
			}
			
			
			if(event.getTags().contains(topTags.get(event.getVenue_id())))			//if tags match, this event is system suggested
			{
				outputKey.set(SYSTEM_TOP);											//output this as SYSTEM-TOP event
				output.collect(outputKey, event);
			} else {
				outputKey.set(SYSTEM_LOW);											//else output this as SYSTEM-LOW event
				output.collect(outputKey, event);
			}
			
			
			//find if the user likes this kind of event
			for( int i=0; i < personData.size(); i++)
			{
				Person p = personData.get(i);
				StringTokenizer eventTagsTokens = new StringTokenizer(event.getTags(), ",");
				boolean userLikes = false;
				while(eventTagsTokens.hasMoreTokens())
				{
					if(p.getLikes().contains(eventTagsTokens.nextToken()))			//the person's likes match this event's tags!
					{
						userLikes = true;
						outputKey.set(p.getName() + USER_TOP);
						output.collect(outputKey, event);
						break;
					}
					
				}
				
				if(!userLikes)		//this means we checked all tags and user does not have this event as his favourite
				{
					outputKey.set(p.getName() + USER_LOW);
					output.collect(outputKey, event);
				}
					
			}
			
		} catch(Exception e)
		{
			System.out.println("Caught Exception: "+e);
			e.printStackTrace();
		}
		
		
	}
}
