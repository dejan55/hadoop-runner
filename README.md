hadoop-runner
=============

This project is used to run a hadoop **MapReduce** Job on a remote server, located in a private network. In this project we relay on traffic **tunneling** to the external server. 

In order to use this project, the hadoop specific parameters should be configured first int the `mk.ukim.finki.etnc.hadoop.conf.properties` file. All configurations should be with the hostnames as in the example. 

Next, the operating system hosts must be configured in order to resolve the hadoop masters and slaves to the loopback address. The example configuration is given in `mk.ukim.finki.etnc.hadoop.hosts-patch`

And the ports that need to be forwarded (for our case) are givven in the `mk.ukim.finki.etnc.hadoop.tunneling-config` file. 

The `mk.ukim.finki.etnc.hadoop.JobRunner` class runs the official WordCount Map and Reduce tasks on the remote server. The Mapper and Reducer classes should be packaed in jar, and that jar should be added into the build path. This is because the JobClient should uppload the classes before it runs them. 
