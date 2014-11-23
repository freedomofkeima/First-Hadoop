package core;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for Hadoop First Project
 * 
 * @author Iskandar Setiadi
 * @version 0.1, by IS @since November 23, 2014
 *
 */

public class MyReducer extends
		Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	private DoubleWritable result = new DoubleWritable();

	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		double total_person = 0, total_entity = 0;
		for (DoubleWritable val : values) {
			total_person += val.get();
			total_entity += 1;
		}
		result.set(total_person / total_entity);
		context.write(key, result);
	}

}
