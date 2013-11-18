package hadoop.ufosight;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

public class UfoSightCount {

	public static class Map
			extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text columnText = new Text();
		private Text badlineKey = new Text("badline");
		private Text totalKey = new Text("total");

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(totalKey, one);

			String[] collumns = value.toString().split("\\t");
			if (collumns.length != 6) {
				context.write(badlineKey, one);
			} else {
				writeMap(context, collumns, 0, "sighted");
				writeMap(context, collumns, 1, "recorded");
				writeMap(context, collumns, 2, "location");
				writeMap(context, collumns, 3, "shape");
				writeMap(context, collumns, 4, "duration");
				writeMap(context, collumns, 5, "description");
			}
		}

		private void writeMap(Context context, String[] collumns, int index,
				String collumnKey) throws IOException, InterruptedException {
			if (isNotEmpty(collumns[index])) {

				columnText.set(collumnKey);
				context.write(columnText, one);
			}
		}

		private boolean isNotEmpty(String string) {

			return string != null && !string.trim().equals("");
		}

	}

	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance();
		job.setJobName("UfoSightCount");
		job.setJarByClass(UfoSightCount.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
