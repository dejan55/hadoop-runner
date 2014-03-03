package mk.ukim.finki.etnc.hadoop;

import mk.ukim.finki.etnc.hadoop.wc.WordCountMapper;
import mk.ukim.finki.etnc.hadoop.wc.WordCountReducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class JobRunner extends FinkiHadoopDriver {

	public void configure(JobConf conf) {
		conf.setJobName("wordcount");

		// key/value of your reducer output
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		// here you have to put your mapper,combiner and reducer class
		conf.setMapperClass(WordCountMapper.class);
		conf.setCombinerClass(WordCountReducer.class);
		conf.setReducerClass(WordCountReducer.class);

		// conf.set("tmpfiles","local_path1,local_path2...");
		// conf.set("tmpjars","local_path1,local_path2...");
		// conf.set("tmparchives","local_path1,local_path2...");

		// this is setting the format of your input, can be
		// TextInputFormat
		conf.setInputFormat(TextInputFormat.class);
		// same with output
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setJarByClass(WordCountMapper.class);

		FileInputFormat.setInputPaths(conf, new Path("/test1.txt"));
		FileOutputFormat.setOutputPath(conf, new Path("/tmp/ristes/riki6"));
	}

	public static void main(String[] args) throws Exception {
		new JobRunner().run("riste.stojanov");
	}

}
