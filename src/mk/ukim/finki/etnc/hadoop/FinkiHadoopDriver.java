package mk.ukim.finki.etnc.hadoop;

import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;

public class FinkiHadoopDriver {

	public void configure(JobConf conf) {
		throw new IllegalStateException(
				"This is an empty implementation. This class must bi overriden");
	}

	private void validate(JobConf conf) {
		if (conf.get("mapred.jar") == null) {
			//
			throw new IllegalArgumentException(
					"You must call conf.setJarByClass(<Class>). If you called it, make sure that the class is in a jar added as a dependency of this project. ");
		}
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

				// the remote machine username
				conf.set("hadoop.job.ugi", username);

				configure(conf);
				validate(conf);

				JobClient.runJob(conf);

				return null;
			}
		});
	}
}
