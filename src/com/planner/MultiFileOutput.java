package com.planner;

import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

import com.vob.EventInfo;

/**
 * Overrides generateFileNameForKeyValue() method to return customized file name for each person and System.
 * @author nikhilrane
 *
 */
public class MultiFileOutput extends MultipleTextOutputFormat<Text, EventInfo>
{
	/**
	 * Overridden method to return filename for each user and System.
	 */
	@Override
	protected String generateFileNameForKeyValue(Text key, EventInfo value, String name)
	{
		StringTokenizer keyTokens = new StringTokenizer(key.toString(), "|");
		return keyTokens.nextToken();
	}
}
