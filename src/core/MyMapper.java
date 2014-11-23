package core;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper for Hadoop First Project
 * 
 * @author Iskandar Setiadi
 * @version 0.1, by IS @since November 23, 2014
 *
 */
public class MyMapper extends Mapper<Object, Text, Text, DoubleWritable> {

	private final static DoubleWritable valueOutput = new DoubleWritable();
	private Text keyOutput = new Text();
	private Pattern p = Pattern.compile("<author>");
	private Pattern p2 = Pattern.compile("<year>(\\w+)</year>");
	private Pattern p3 = Pattern
			.compile("</article>|</inproceedings>|</proceedings>|</book>|</incollection>|</phdthesis>|</mastersthesis>|</www>");
	private Matcher m;
	private double res = 0;

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		m = p2.matcher(value.toString());
		while (m.find()) {
			String find = m.group(1);
			keyOutput.set(find);
		}
		m = p.matcher(value.toString());
		while (m.find()) {
			res = res + 1;
		}
		m = p3.matcher(value.toString());
		if (m.find()) {
			valueOutput.set(res); // set result
			context.write(keyOutput, valueOutput);
			keyOutput.set("");
			res = 0;
		}
	}

}
