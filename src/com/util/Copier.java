package com.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

/**
 * Class to copy data as editors are failing due to large amount of records.
 * @author nikhilrane
 *
 */
public class Copier {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception
	{
		
		BufferedReader in = new BufferedReader(new FileReader("/home/nikhilrane/Downloads/RS/events_music.csv"));
		BufferedWriter out = new BufferedWriter(new FileWriter("/home/nikhilrane/Downloads/RS/output.csv"));
		String line;
		long ctr=0;
		
		while((line = in.readLine()) != null && ctr < 500000)
		{
			
			out.write(line);
			ctr++;
		}
		System.out.println("Done!");
		
		in.close();
		out.close();
	}

}
