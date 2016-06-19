package com.github.bskaggs.avro_json_hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/**
 * {@link RecordReader} that emits (@{link Text}, @{link {@link NullWritable}) pairs.
 * It wraps a {@line LineRecordReader}, so it inherits all its configuration.
 * 
 * @author bskaggs
 *
 */
public class TextLineRecordReader extends RecordReader<Text, NullWritable> {
	
	private LineRecordReader inner = new LineRecordReader();

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		inner.initialize(split, context);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return inner.nextKeyValue();
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return inner.getCurrentValue();
	}

	@Override
	public NullWritable getCurrentValue() throws IOException, InterruptedException {
		return NullWritable.get();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return inner.getProgress();
	}

	@Override
	public void close() throws IOException {
		inner.close();
	}
}
