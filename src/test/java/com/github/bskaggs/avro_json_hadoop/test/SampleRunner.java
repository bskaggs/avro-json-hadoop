package com.github.bskaggs.avro_json_hadoop.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.github.bskaggs.avro_json_hadoop.AvroAsJsonKeyInputFormat;

/**
 * Runs a map-only job that converts Avro to line-delimited JSON.
 * @author brad
 *
 */
public class SampleRunner extends Configured implements Tool {

	public int run(String[] args) throws Exception {
        // Configuration processed by ToolRunner
        Configuration conf = getConf();
        
        // Create a JobConf using the processed conf
        Job job = Job.getInstance(conf);
        
        // Process custom command-line options
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        
        // Specify various job-specific parameters     
        job.setJobName("AVRO JSON Test");
        job.setInputFormatClass(AvroAsJsonKeyInputFormat.class);
        AvroAsJsonKeyInputFormat.setInputPaths(job, in);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, out);
        job.setNumReduceTasks(0);

        job.submit();
        return job.waitForCompletion(true) ? 0 : -1;
      }
      
      public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SampleRunner(), args);
        System.exit(res);
      }
}
