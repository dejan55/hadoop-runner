package mk.ukim.finki.etnc.hadoop;

import mk.ukim.finki.etnc.hadoop.xml.XmlInputFormat;
import mk.ukim.finki.etnc.wikidump.WikiDumpParseMapper;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;

public class WikiDumpParserRunner extends FinkiHadoopDriver {

	public void configure(JobConf conf) throws Exception {
		conf.setJobName("wiki-dump-parser-2");

		// key/value of your reducer output
		conf.setOutputKeyClass(Text.class); // wikipage uri
		conf.setOutputValueClass(Text.class); // wikipage content

		// here you have to put your mapper,combiner and reducer class
		conf.setMapperClass(WikiDumpParseMapper.class);
		// conf.setCombinerClass(WordCountReducer.class);
		// conf.setReducerClass(WordCountReducer.class);

		// We have XML input
		conf.setInputFormat(XmlInputFormat.class);
		// transformed into text
		conf.setOutputFormat(TextOutputFormat.class);

		// set the jar
		conf.setJarByClass(WikiDumpParseMapper.class);

		FileInputFormat.setInputPaths(conf, new Path(
				"/tmp/ristes/small_wikidump.xml"));

		setOutput("/tmp/ristes/parse.out");

		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");

		setLibraryJarsTempPath(conf, "/tmp/ristes/lib/");

		addLibraryJarLocalPath("lib/jdom-1.1.3.jar");

	}

	public static void main(String[] args) throws Exception {
		new WikiDumpParserRunner().run("riste.stojanov");
	}

}