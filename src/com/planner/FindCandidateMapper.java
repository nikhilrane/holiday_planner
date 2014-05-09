package com.planner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Timestamp;
import java.util.ArrayList;
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
 * Mapper class which takes input one event information, parses it, checks if it fits for all persons (distance, start and end times) and outputs if satisfied.
 * @author nikhilrane
 *
 */
public class FindCandidateMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, EventInfo> 
{
	private Text venue_ID = new Text();
	private Path localPath[];
	private static ArrayList<Person> personData = new ArrayList<Person>();
	
	/**
	 * Gets cached files.
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
	
	
	/**
	 * Returns the distance between two given latitudes and longitudes.
	 * 
	 * @param latitude
	 * @param longitude
	 * @param sourceLatitude
	 * @param sourceLongitude
	 * @return
	 */
	public long calculateDistance(double latitude, double longitude, double sourceLatitude, double sourceLongitude)
	{
		long distance = -1;
		
		double sourceLatRad = Math.toRadians(sourceLatitude);
		double sourceLonRad = Math.toRadians(sourceLongitude);
		double latRad = Math.toRadians(latitude);
		double lonRad = Math.toRadians(longitude);
		
		distance = Math.round(3959 * (Math.acos(Math.cos(sourceLatRad) * Math.cos(latRad) * Math.cos(lonRad - sourceLonRad) + Math.sin(sourceLatRad) * Math.sin(latRad))));

		return distance;
	}
	

	
	/**
	 * Mapper to output candidate event records based on distance from all persons, start and end times of event and persons.
	 */
	public void map(LongWritable key, Text value, OutputCollector<Text, EventInfo> output, Reporter reporter) 
	{
		try
		{
			
			String line;
			EventInfo event = new EventInfo();
			
			//let's get our persons data
			if(personData.isEmpty())
			{
				BufferedReader in = new BufferedReader(new FileReader(localPath[0].toString()));
				while((line = in.readLine()) != null)
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
				in.close();											//file reading is over so close file reader object!
			}
			
			
			line = value.toString();								//got one event information
			StringTokenizer eventTokens = new StringTokenizer(line, "|");
			
			//initialize our EventInfo object
			if(eventTokens.hasMoreTokens())
			{
				//ACTUAL: id|title|start_time|stop_time|venue_name|latitude|longitude|description|category|recur_string|created|modified|owner|url|tags|venue_id|gotvenue
				//USED:   id|title|start_time|stop_time|venue_name|latitude|longitude|description|category|recur_string|url|tags|venue_id
				event.setId(eventTokens.nextToken());
				event.setTitle(eventTokens.nextToken());
				
				String startTime = eventTokens.nextToken();
				if(startTime.contains("+"))							//filtering unnecessary data
					startTime = startTime.substring(0, startTime.length()-3);
				
				event.setStart_time(startTime);
				
				String stopTime = eventTokens.nextToken();
				if(stopTime.contains("+"))							//filtering unnecessary data
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
				
				//skip 3 tokens as we do not use them.
				eventTokens.nextToken();
				eventTokens.nextToken();
				eventTokens.nextToken();
				
				event.setUrl(eventTokens.nextToken());
				event.setTags(eventTokens.nextToken());
				event.setVenue_id(eventTokens.nextToken());
			}
			
			//we are here means we initialized EventInfo object
			boolean isCandidate = false;
			
			for(int i=0; i < personData.size(); i++)			//for each person, check for his/her radius and start & end times
			{
				Person p = personData.get(i);
				
				//if distance is more than this person allows, break and leave this event!
				if(p.isWithinDistance(calculateDistance(event.getLatitude(), event.getLongitude(), p.getLatitude(), p.getLongitude()))
						&& p.isWithinTimeFrame(Timestamp.valueOf(event.getStart_time())))
				{
					isCandidate = true;
				} else {
					isCandidate = false;
					break;
				}
			}
			
			//if this event is within radius and time frame of all people, add it as a candidate event
			if (isCandidate)
			{
				venue_ID.set(event.getVenue_id());
				output.collect(venue_ID, event);
			}
			
		} catch(Exception e)
		{
			System.out.println("Caught Exception: "+e);
		}
		
	}
}
