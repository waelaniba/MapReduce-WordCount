package com.ov.training.v1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StubReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int sum = 0;

		// For each key value pair, get the value and adds to the sum
		// to get the total occurances of a word
		for (IntWritable value : values) {
			sum += value.get();
		}

		// Writes the word and total occurances as key-value pair to the context
		context.write(key, new IntWritable(sum));
	}

}
