package com.vob;

import java.sql.Timestamp;

/**
 * Models one person's information. Also provides utility method to check for radius against calculated distance and start/end times.
 * @author nikhilrane
 *
 */
public class Person
{
	//Name|latitude|longitude|startTime|endTime|radius|likes
	private String name;
	private double latitude;
	private double longitude;
	private Timestamp startTime;
	private Timestamp endTime;
	private long radius;
	private String likes;
	
	public Person()	{}

	public Person(String name, double latitude, double longitude,
			Timestamp startTime, Timestamp endTime, long radius, String likes) {
		super();
		this.name = name;
		this.latitude = latitude;
		this.longitude = longitude;
		this.startTime = startTime;
		this.endTime = endTime;
		this.radius = radius;
		this.likes = likes;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
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

	public Timestamp getStartTime() {
		return startTime;
	}

	public void setStartTime(Timestamp startTime) {
		this.startTime = startTime;
	}

	public Timestamp getEndTime() {
		return endTime;
	}

	public void setEndTime(Timestamp endTime) {
		this.endTime = endTime;
	}

	public long getRadius() {
		return radius;
	}

	public void setRadius(long radius) {
		this.radius = radius;
	}

	public String getLikes() {
		return likes;
	}

	public void setLikes(String likes) {
		this.likes = likes;
	}
	
	/**
	 * Returns true if the given distance is within radius of *this* person, else returns false.
	 * @param distance
	 * @return
	 */
	public boolean isWithinDistance(long distance)
	{
		return (distance >=0 && distance <= radius);
	}
	
	
	/**
	 * Return true if the given event's start time is within time frame of *this* person, else returns false.
	 * @param eventStartTime
	 * @return
	 */
	public boolean isWithinTimeFrame(Timestamp eventStartTime)
	{
		return (startTime.before(eventStartTime) && endTime.after(eventStartTime));
	}
	
	

}
