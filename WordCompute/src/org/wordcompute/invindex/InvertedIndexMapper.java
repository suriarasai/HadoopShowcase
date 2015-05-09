package org.wordcompute.invindex;

import java.io.IOException;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	private Text word = new Text();
	private Text filename = new Text();

	private boolean caseSensitive = false;

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String filenameStr = ((FileSplit) context.getInputSplit()).getPath().getName();
		filename = new Text(filenameStr);
		
		String line = value.toString();

		if (!caseSensitive) {
			line = line.toLowerCase();
		}

		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());			
			context.write(word, filename);
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		this.caseSensitive = conf.getBoolean("wordcount.case.sensitive",false);
	}
}