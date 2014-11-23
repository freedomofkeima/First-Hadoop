/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package core;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * Reads records that are delimited by a specifc begin/end tag. Modified by:
 * Iskandar Setiadi
 * 
 * @version 0.1, by IS @since November 23, 2014
 */
public class XmlInputFormat extends TextInputFormat {

	public static final String START_TAG_KEY = "xmlinput.start";

	public XmlRecordReader getRecordReader(InputSplit inputSplit,
			TaskAttemptContext taskAttemptContent) throws IOException {
		return new XmlRecordReader();
	}

	/**
	 * XMLRecordReader class to read through a given xml document to output xml
	 * blocks as records as specified by the start tag and end tag
	 * 
	 */
	public static class XmlRecordReader extends
			RecordReader<LongWritable, Text> {
		private ArrayList<ArrayList<Byte>> tag;
		private byte[] startTag;
		private long start;
		private long end;
		private FSDataInputStream fsin;
		private final DataOutputBuffer buffer = new DataOutputBuffer();
		private LongWritable key;
		private Text value;

		@SuppressWarnings("unchecked")
		@Override
		public void initialize(InputSplit split,
				TaskAttemptContext taskAttemptContent) throws IOException,
				InterruptedException {
			startTag = taskAttemptContent.getConfiguration().get(START_TAG_KEY)
					.getBytes("utf-8");
			key = new LongWritable();
			value = new Text();

			tag = new ArrayList<ArrayList<Byte>>();
			ArrayList<Byte> tempTag = new ArrayList<Byte>();
			for (int i = 0; i < startTag.length; i++) {
				Byte temp = startTag[i];
				if (temp.equals('|')) {
					tag.add((ArrayList<Byte>) tempTag.clone());
					tempTag.clear();
				} else {
					tempTag.add(temp);
				}
			}

			// open the file and seek to the start of the split
			start = ((FileSplit) split).getStart();
			end = start + split.getLength();
			Path file = ((FileSplit) split).getPath();
			FileSystem fs = file.getFileSystem(taskAttemptContent
					.getConfiguration());
			fsin = fs.open(((FileSplit) split).getPath());
			fsin.seek(start);
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			return key;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (fsin.getPos() < end) {
				if (readUntilMatch(tag, false)) {
					try {
						buffer.write('<'); // opening tag
						buffer.write(startTag);
						if (readUntilMatch(tag, true)) {
							buffer.write('>'); // closing tag
							key.set(fsin.getPos());
							value.set(buffer.getData(), 0, buffer.getLength());
							return true;
						}
					} finally {
						buffer.reset();
					}
				}
			}
			return false;
		}

		@Override
		public void close() throws IOException {
			fsin.close();
		}

		@Override
		public float getProgress() throws IOException {
			return (fsin.getPos() - start) / (float) (end - start);
		}

		private boolean readUntilMatch(ArrayList<ArrayList<Byte>> match,
				boolean withinBlock) throws IOException {
			int i = 0;
			while (true) {
				int b = fsin.read();
				// end of file:
				if (b == -1)
					return false;
				// save to buffer:
				if (withinBlock)
					buffer.write(b);

				// check if we're matching:
				for (ArrayList<Byte> ch : match) {
					if (ch.get(i).equals(b)) {
						i++;
						if (i >= ch.size())
							return true;
					} else
						i = 0;
				}
				// see if we've passed the stop point:
				if (!withinBlock && i == 0 && fsin.getPos() >= end)
					return false;
			}
		}
	}
}