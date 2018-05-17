import java.io.IOException;
import java.text.DecimalFormat;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Hw2Part1 class
 * 
 */
public class Hw2Part1 {

	/**
	 * TokenizerMapper class
	 */
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		private Text mk = new Text();
		private Text mv = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// split the file with '\n' symbol, get input lines
			StringTokenizer lineitr = new StringTokenizer(value.toString(), "\n");

			// if has more lines
			while (lineitr.hasMoreTokens()) {
				String lineStr = lineitr.nextToken();
				StringTokenizer tokenitr = new StringTokenizer(lineStr);
				
				// if current line has less or more than 3 tokens, ignore this line
				if (tokenitr.countTokens() != 3) {
					continue;
				} 
				else {
					String source = tokenitr.nextToken();
					String destination = tokenitr.nextToken();
					String talktime = tokenitr.nextToken();
					
					// if the talktime value of current line is not a number, ignore this line 
					String regex = "^[-\\+]?[0-9]*(\\.?)[0-9]*$";
					Pattern reg = Pattern.compile(regex);
					if (!(reg.matcher(talktime).matches())) {
						continue;
					}
					
					float realtime = Float.valueOf(talktime);
					
					mk.set(source + " " + destination);
					mv.set("1 " + Float.toString(realtime));
					context.write(mk, mv);
				}
			}
		}
	}

	/**
	 * TimeAverageCombiner class
	 */
	public static class TimeAverageCombiner extends Reducer<Text, Text, Text, Text> {
		private Text res = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			float totaltime = 0;
			int totalcount = 0;
			for (Text val : values) {
				String str = val.toString();
				StringTokenizer stritr = new StringTokenizer(str);
				String counttoken = stritr.nextToken();
				String timetoken = stritr.nextToken();
				
				int count = Integer.valueOf(counttoken);
				float linetime = Float.valueOf(timetoken);
				
				totaltime += count * linetime;
				totalcount += count;
			}
			
			float avgtime = totaltime / totalcount;
			res.set(Integer.toString(totalcount) + " " + Float.toString(avgtime));
			context.write(key, res);
		}
	}

	/**
	 * TimeAverageReducer class
	 */
	public static class TimeAverageReducer extends Reducer<Text, Text, Text, Text> {

		private Text result_key = new Text();
		private Text result_value = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			float totaltime = 0;
			int totalcount = 0;
			for (Text val : values) {
				String str = val.toString();
				StringTokenizer stritr = new StringTokenizer(str);
				String counttoken = stritr.nextToken();
				String timetoken = stritr.nextToken();
				
				int count = Integer.valueOf(counttoken);
				float linetime = Float.valueOf(timetoken);
				
				totaltime += count * linetime;
				totalcount += count;
			}
			
			DecimalFormat df = new DecimalFormat("#.000");
			String realtimestr = df.format((totaltime * 1000) / (totalcount * 1000.0));

			// generate result key and value
			result_key.set(key);
			result_value.set(Integer.toString(totalcount) + " " + realtimestr);

			// write the context
			context.write(result_key, result_value);
		}
	}

	/**
	 * This is the main function
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator", " ");
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length < 2) {
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "calculate talk-time average");

		job.setJarByClass(Hw2Part1.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(TimeAverageCombiner.class);
		job.setReducerClass(TimeAverageReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// add the input paths as given by command line
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}

		// add the output path as given by the command line
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
