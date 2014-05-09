package com.vob;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Models one event's information. Also provides utility methods to return pretty output to be used by Reducer.
 * @author nikhilrane
 *
 */
public class EventInfo implements Writable
{
	//id|title|start_time|stop_time|venue_name|latitude|longitude|description|category|recur_string|created|modified|owner|url|tags|venue_id|gotvenue
	//USED: id|title|start_time|stop_time|venue_name|latitude|longitude|description|category|recur_string|url|tags|venue_id
	private String id;
	private String title;
	private String start_time;
	private String stop_time;
	private String venue_name;
	private double latitude;
	private double longitude; 
	private String description;
	private String category;
	private String recur_string;
	private String url;
	private String tags;
	private String venue_id;
	
	public EventInfo() {}
	
	
	public EventInfo(String id, String title, String start_time,
			String stop_time, String venue_name, double latitude,
			double longitude, String description, String category,
			String recur_string, String url, String tags, String venue_id)
	{
		this.id = id;
		this.title = title;
		this.start_time = start_time;
		this.stop_time = stop_time;
		this.venue_name = venue_name;
		this.latitude = latitude;
		this.longitude = longitude;
		this.description = description;
		this.category = category;
		this.recur_string = recur_string;
		this.url = url;
		this.tags = tags;
		this.venue_id = venue_id;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getStart_time() {
		return start_time;
	}
	public void setStart_time(String start_time) {
		this.start_time = start_time;
	}
	public String getStop_time() {
		return stop_time;
	}
	public void setStop_time(String stop_time) {
		this.stop_time = stop_time;
	}
	public String getVenue_name() {
		return venue_name;
	}
	public void setVenue_name(String venue_name) {
		this.venue_name = venue_name;
	}
	public double getLatitude() {
		return latitude;
	}
	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}
	public double getLongitude() {
		return longitude;
	}
	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getCategory() {
		return category;
	}
	public void setCategory(String category) {
		this.category = category;
	}
	public String getRecur_string() {
		return recur_string;
	}
	public void setRecur_string(String recur_string) {
		this.recur_string = recur_string;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getTags() {
		return tags;
	}
	public void setTags(String tags) {
		this.tags = tags;
	}
	public String getVenue_id() {
		return venue_id;
	}
	public void setVenue_id(String venue_id) {
		this.venue_id = venue_id;
	}
	
	/**
	 * Has to be overridden so Hadoop can read data.
	 */
	@Override
	public void readFields(DataInput in) throws IOException
	{
		id = in.readUTF();
		title = in.readUTF();
		start_time = in.readUTF();
		stop_time = in.readUTF();
		venue_name = in.readUTF();
		latitude = in.readDouble();
		longitude = in.readDouble();
		description = in.readUTF();
		category = in.readUTF();
		recur_string = in.readUTF();
		url = in.readUTF();
		tags = in.readUTF();
		venue_id = in.readUTF();
		
	}
	
	/**
	 * Has to be overridden so Hadoop can write data.
	 */
	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeUTF(id);
		out.writeUTF(title);
		out.writeUTF(start_time);
		out.writeUTF(stop_time);
		out.writeUTF(venue_name);
		out.writeDouble(latitude);
		out.writeDouble(longitude);
		out.writeUTF(description);
		out.writeUTF(category);
		out.writeUTF(recur_string);
		out.writeUTF(url);
		out.writeUTF(tags);
		out.writeUTF(venue_id);
	}
	
	/**
	 * Overridden to provide pretty output. This method is called when Reducer calls output() which is written to file.
	 */
	@Override
	public String toString()
	{
		return id+ "|"+title+ "|"+start_time+ "|"+stop_time+ "|"+venue_name+ "|"+latitude+ "|"+longitude+ "|"+description+ "|"+category+ "|"+recur_string+ "|"+url+ "|"+tags+ "|"+venue_id;
	}

	
	
}
