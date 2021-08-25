## Installation on Ubuntu
- Installing docker on ubunti
	- sudo apt-get install gnupg
	- https://docs.docker.com/engine/install/ubuntu/
	sudo apt-get install gnupg

- Install Hadoop
	- sudo apt-get install openssh-server openssh-client
	- ssh-keygen -t rsa -P ""
	- cat $HOME/.ssh/id_rsa.pub >> $HOME/.ssh/authorized_keys
	- wget -q https://downloads.apache.org/hadoop/common/hadoop-3.3.0/hadoop-3.3.0.tar.gz
	- tar xzf hadoop-3.3.0.tar.gz
	- 
```shell
vi ~/.bashrc
export HADOOP_HOME=$HOME/hadoop-3.3.0
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
```

## Installation Path
```shell
ls /usr/hdp/
```
## Version

```shell
hadoop version 
Hadoop 2.7.3.2.5.0.0-1245
Subversion git@github.com:hortonworks/hadoop.git -r cb6e514b14fb60e9995e5ad9543315cd404b4e59
Compiled by jenkins on 2016-08-26T00:55Z
Compiled with protoc 2.5.0
From source with checksum eba8ae32a1d8bb736a829d9dc18dddc2
This command was run using /usr/hdp/2.5.0.0-1245/hadoop/hadoop-common-2.7.3.2.5.0.0-1245.jar
```
