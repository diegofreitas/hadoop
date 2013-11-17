package org.myorg;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.TokenCounterMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

public class WordCount {

	/*public static class Map
			extends
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] words = value.toString().split(" ");
			for (String str : words) {
				word.set(str);
				context.write(word, one);
			}
		}

	}*/

	/*public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int total = 0;
			Iterator<IntWritable> iterator = values.iterator();
			while(iterator.hasNext()){
				total+=iterator.next().get();
			}
			context.write(key, new IntWritable(total));
		}
	}*/

	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance();
		job.setJobName("wordcount__");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenCounterMapper.class);
		job.setReducerClass(IntSumReducer.class);
		//job.setMapperClass(Map.class);
		//job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}