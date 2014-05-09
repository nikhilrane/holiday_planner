package com.planner;

import java.net.URI;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.vob.EventInfo;


/**
 * The main function to create, schedule and start jobs.
 * @author nikhilrane
 *
 */
public class PlannerMain 
{
	public static void main(String[] args) throws Exception 
	{

		//This Map-Combine-Reduce job finds events which are candidates i.e. which lie within given radius and time frame.
		JobConf plannerJob = new JobConf(PlannerMain.class);
		plannerJob.setJobName("FindCandidates");
		
		DistributedCache.addCacheFile(new URI("/home/nikhilrane/Downloads/RS/persons_data.txt"), plannerJob);

		plannerJob.setMapOutputKeyClass(Text.class);
		plannerJob.setMapOutputValueClass(EventInfo.class);
		plannerJob.setOutputKeyClass(Text.class);
		plannerJob.setOutputValueClass(EventInfo.class);

		plannerJob.setMapperClass(FindCandidateMapper.class);
		plannerJob.setCombinerClass(FindCandidateCombiner.class);
		plannerJob.setReducerClass(FindCandidateReducer.class);

		plannerJob.setInputFormat(TextInputFormat.class);
		plannerJob.setOutputFormat(TextOutputFormat.class);
		
		plannerJob.set("mapred.textoutputformat.separator", "|");

		FileInputFormat.setInputPaths(plannerJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(plannerJob, new Path(args[1]));

		JobClient.runJob(plannerJob);
		

		//This Map-Combine-Reduce job finds the trend. The input is venues which are sorted out in previous FindCandidate job.
		JobConf findTrendJob = new JobConf(PlannerMain.class);
		findTrendJob.setJobName("FindTrend");
		
		//Add files to cache which are needed
		DistributedCache.addCacheFile(new URI("/home/nikhilrane/Downloads/RS/persons_data.txt"), findTrendJob);
		DistributedCache.addCacheFile(new URI("/home/nikhilrane/workspace_hadoop/Planner/output/part-00000"), findTrendJob);
		

		findTrendJob.setOutputKeyClass(Text.class);
		findTrendJob.setOutputValueClass(IntWritable.class);

		findTrendJob.setMapperClass(FindTrendMapper.class);
		findTrendJob.setCombinerClass(FindTrendCombiner.class);
		findTrendJob.setReducerClass(FindTrendReducer.class);

		findTrendJob.setInputFormat(TextInputFormat.class);
		findTrendJob.setOutputFormat(TextOutputFormat.class);
		
		
		findTrendJob.set("mapred.textoutputformat.separator", "|");

		FileInputFormat.setInputPaths(findTrendJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(findTrendJob, new Path(args[2]));

		JobClient.runJob(findTrendJob);
		
		
		//This Map-Reduce job finds the ranking based on tags the user likes and the trend at given venue.
		JobConf rankResultsJob = new JobConf(PlannerMain.class);
		rankResultsJob.setJobName("RankResults");
		
		//Add files to cache which are needed
		DistributedCache.addCacheFile(new URI("/home/nikhilrane/Downloads/RS/persons_data.txt"), rankResultsJob);
		DistributedCache.addCacheFile(new URI("/home/nikhilrane/workspace_hadoop/Planner/outputTrend/part-00000"), rankResultsJob);
		
		rankResultsJob.setNumMapTasks(1);
		rankResultsJob.setNumReduceTasks(1);
		
		rankResultsJob.setMapOutputKeyClass(Text.class);
		rankResultsJob.setMapOutputValueClass(EventInfo.class);
		rankResultsJob.setOutputKeyClass(Text.class);
		rankResultsJob.setOutputValueClass(EventInfo.class);

		rankResultsJob.setMapperClass(RankResultsMapper.class);
		rankResultsJob.setCombinerClass(RankResultsCombiner.class);
		rankResultsJob.setReducerClass(RankResultsReducer.class);

		rankResultsJob.setInputFormat(TextInputFormat.class);
		rankResultsJob.setOutputFormat(TextOutputFormat.class);
		
		//Create files as per System recommended events and user likes.
		rankResultsJob.setOutputFormat(MultiFileOutput.class);
		
		rankResultsJob.set("mapred.textoutputformat.separator", "|");

		FileInputFormat.setInputPaths(rankResultsJob, new Path(args[1]));
		FileOutputFormat.setOutputPath(rankResultsJob, new Path(args[3]));

		JobClient.runJob(rankResultsJob);
		
	}
}
