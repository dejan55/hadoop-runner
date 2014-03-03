package mk.ukim.finki.etnc.hadoop;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;

public class FinkiHadoopDriver {

	private static final String JARS_TARGET = "jars_target";

	public void addLibraryJarLocalPath(String path) throws IOException {
		String target = Context.conf.get(JARS_TARGET);
		if (target == null || target.isEmpty()) {
			throw new IllegalStateException("Must set the " + JARS_TARGET
					+ " configuration paramether. Use the method: setLibraryJarsTempPath");
		}
		Path remotePath = new Path(target
				+ path.substring(path.lastIndexOf("/") + 1));
		Context.hdfs.copyFromLocalFile(new Path(path), remotePath);
		DistributedCache.addArchiveToClassPath(remotePath, Context.conf,
				Context.hdfs);
	}

	public void configure(JobConf conf) throws Exception {
		throw new IllegalStateException(
				"This is an empty implementation. This class must bi overriden");
	}

	public void setLibraryJarsTempPath(JobConf conf, String tempPath) {
		conf.set(JARS_TARGET, tempPath);
	}

	public void copyFiles(JobConf conf, FileSystem hdfs) throws IOException {
	}

	private void validate(JobConf conf) {
		if (conf.get("mapred.jar") == null) {
			//
			throw new IllegalArgumentException(
					"You must call conf.setJarByClass(<Class>). If you called it, make sure that the class is in a jar added as a dependency of this project. ");
		}
	}

	public void setOutput(String path) throws IOException {
		int i = 0;
		String uniquePath = path;
		while (Context.hdfs.exists(new Path(uniquePath))) {
			uniquePath = path + i;
			i++;
		}
		FileOutputFormat.setOutputPath(Context.conf, new Path(uniquePath));
	}

	private static class Context {
		public static JobConf conf;
		public static FileSystem hdfs;
	}

	public final void run(final String username) throws Exception {

		UserGroupInformation ugi = UserGroupInformation
				.createRemoteUser(username);

		final Properties prop = new Properties();
		prop.load(FinkiHadoopDriver.class
				.getResourceAsStream("config.properties"));
		
	

		ugi.doAs(new PrivilegedExceptionAction<Void>() {

			public Void run() throws Exception {
				// create a configuration
				JobConf conf = new JobConf();
				for (Object key : prop.keySet()) {
					String sKey = (String) key;
					conf.set(sKey, (String) prop.getProperty(sKey));
				}
				FileSystem hdfs = FileSystem.get(conf);
				Context.conf = conf;
				Context.hdfs = hdfs;

				// the remote machine username
				conf.set("hadoop.job.ugi", username);

				configure(conf);

				// copy the required resources to the remote fs
				copyFiles(conf, hdfs);

				validate(conf);

				Context.conf = null;
				Context.hdfs = null;

				JobClient.runJob(conf);
				return null;
			}
		});
	}
}
