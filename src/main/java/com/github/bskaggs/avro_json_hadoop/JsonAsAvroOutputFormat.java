package com.github.bskaggs.avro_json_hadoop;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class JsonAsAvroOutputFormat<K, V> extends OutputFormat<Text, V> {
	private AvroKeyOutputFormat<K> inner;
	
	@Override
	public RecordWriter<Text, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		final RecordWriter<AvroKey<K>, NullWritable> writer = inner.getRecordWriter(context);
	    Configuration conf = context.getConfiguration();
	    
	    final Schema writerSchema = getWriterSchema(context, conf);
	    
		return new RecordWriter<Text, V>() {
			private K datum = null;
			@Override
			public void write(Text key, V value) throws IOException, InterruptedException {
				JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(writerSchema, key.toString());
				GenericDatumReader<K> reader = new GenericDatumReader<K>(writerSchema);
				datum = reader.read(datum, jsonDecoder);
				writer.write(new AvroKey<K>(datum), NullWritable.get());
			}

			@Override
			public void close(TaskAttemptContext context) throws IOException, InterruptedException {
				writer.close(context);
			}
		};
	}

	private Schema getWriterSchema(TaskAttemptContext context, Configuration conf) {
		if (context.getNumReduceTasks() == 0) {
			Schema schema = AvroJob.getMapOutputKeySchema(conf);
			if (schema != null) {
				return schema;
			}
		}
		return AvroJob.getOutputKeySchema(conf);
	}

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
		inner.checkOutputSpecs(context);
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
		return inner.getOutputCommitter(context);
	}
}
