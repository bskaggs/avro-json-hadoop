package com.github.bskaggs.avro_json_hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * @(link InputFormat} that will allow input from either Avro files or line-delimited JSON files.
 * 
 * If the file name ends in ".avro", it tries to process it as an Avro file.  If not, it assumes line-delimited JSON.
 * 
 * @author bskaggs
 *
 */
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
