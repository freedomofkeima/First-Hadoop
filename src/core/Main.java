package core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Main class for Hadoop First Project 
 * Usage: input_path output_path
 * 
 * @author Iskandar Setiadi
 * @version 0.1, by IS @since November 23, 2014
 *
 */

public class Main {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		/** Ensure Serialization */
		conf.set(
				"io.serializations",
				"org.apache.hadoop.io.serializer.JavaSerialization,"
						+ "org.apache.hadoop.io.serializer.WritableSerialization");

		conf.set(
				"xmlinput.start",
				"article|inproceedings|proceedings|book|incollection|phdthesis|mastersthesis|www");

		Job job = Job.getInstance(conf, "iskandar-mean-parser");
		job.setJarByClass(Main.class);

		job.setMapperClass(MyMapper.class); 
		job.setCombinerClass(MyReducer.class); 
		job.setReducerClass(MyReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		/** args[0] = Input, args[1] = Output */
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		try {
			job.waitForCompletion(true);
		} catch (InterruptedException | ClassNotFoundException ex) {
			System.out.println(ex);
		}
	}

}
