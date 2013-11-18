package hadoop.ufosight;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UfoShapesAndDuration {

	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable sec = new IntWritable(1);
		private Text columnText = new Text();

		String timepattern = "(.*)(\\d)(\\s*min.*)|(\\s*sec.*)|(\\s*h.*)";

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] collumns = value.toString().split("\\t");
			if (collumns.length == 6 && isNotEmpty(collumns[3])
					&& isNotEmpty(collumns[4])) {

				try {
					columnText.set(collumns[3]);
					sec.set(Integer.valueOf(collumns[4].replaceAll(timepattern,
							"$2")));//everything is the same time unit :)
					context.write(columnText, sec);
				} catch (Exception e) {
					// /failed on Integer.valueOf i dont know if the regex will work for every data
				}

			}
		}

		private boolean isNotEmpty(String string) {
			return string != null && !string.trim().equals("");
		}

	}

	public static class UfoReducer extends
			Reducer<Text, IntWritable, Text, Text> {

		Text result = new Text();
		
		
		protected void reduce(Text arg0, Iterable<IntWritable> arg1, Context context)
				throws IOException, InterruptedException {
			int min = 0;
			int max = 0;
			int total = 0;
			int count = 0;
			for( IntWritable time : arg1){
				count++;
				total += time.get();
				if(min == 0 || time.get() < min){
					min = time.get();
				} 
				if(time.get() > max){
					max = time.get();
				}
			}
			result.set(String.format("count %d, total %d, min %d,max %d, mean %d", count, total,min,max,total/count));
			context.write(arg0, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance();
		job.setJobName("UfoSightCount");
		job.setJarByClass(UfoShapesAndDuration.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(UfoReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
