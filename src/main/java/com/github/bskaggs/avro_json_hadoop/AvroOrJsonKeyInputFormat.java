package com.github.bskaggs.avro_json_hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class AvroOrJsonKeyInputFormat extends FileInputFormat<Text, NullWritable> {
	@Override
	public RecordReader<Text, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		String name = ((FileSplit) split).getPath().getName();
		if (name.endsWith(".avro")) {
			return new AvroAsJsonRecordReader();
		} else {
			return new TextLineRecordReader();
		}
	}
}
