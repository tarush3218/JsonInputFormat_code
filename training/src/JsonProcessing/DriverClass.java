package JsonProcessing;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DriverClass {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException,InterruptedException,ClassNotFoundException{

		JobConf conf = new JobConf();
		conf.set("start_tag", "init-param");
		Job job = Job.getInstance(conf);
		job.setJarByClass(DriverClass.class);
		job.setInputFormatClass(JsonInputFormat.class);
		job.setMapperClass(MapperClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		JsonInputFormat.addInputPath(job, new Path("/home/cloudera/Desktop/data.json"));
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path("/home/cloudera/Desktop/data21.txt"));
		job.waitForCompletion(false);
	}

}
