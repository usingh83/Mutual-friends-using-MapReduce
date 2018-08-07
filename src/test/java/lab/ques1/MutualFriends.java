package lab.ques1;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MutualFriends {

	public static class Map extends
	Mapper<LongWritable, Text, PairWritable, Text> {
		private Text word = new Text();
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] friend = value.toString().split("\t");
			long uID = Long.parseLong(friend[0]);
			if (friend.length > 1) {
				String[] user = friend[1].split(",");
				word.set(friend[1]);
				for (String data : user) {
					context.write(sortPair(uID, Long.parseLong(data)), word);
					}
			}
		}
	}

	public static PairWritable sortPair(Long p1, Long p2) {
		if (p1 > p2) {
			return (new PairWritable(p2, p1));

		} else
			return (new PairWritable(p1, p2));

	}

	public static class Reduce extends
			Reducer<PairWritable, Text, PairWritable, Text> {
		private Text result = new Text();
		public void reduce(PairWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			List<List<String>> fLists = new ArrayList<>();
			for (Text friends : values) {
				fLists.add(Arrays.asList(friends.toString().split(",")));
			}
			List<String> aList = fLists.get(0);
			List<String> bList = fLists.get(1);

			for (String str : bList) {
				if (aList.contains(str))
					sb.append(str + ",");
				}
				if (sb.length() > 0) {
					sb.setLength(sb.length() - 1);
				} else
					sb.append(" ");
			result.set(sb.toString());
			context.write(key, result); 
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: MutualFriends <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "MutualFriends");
		job.setJarByClass(MutualFriends.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(PairWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(PairWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}