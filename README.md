repoHadoop
==========

This repo helps me save me some work uploading MapReduce applications to the remote machine I use to connect to Hadoop, where I've initizalized a git repository.


useful notes:
=============

MapReduce applications require including Hadoop-common and Hadoop-MapReduce java libraries.
I'm getting started with version 2.2.0

	mapred vs. mapreduce
	--------------------
	They're both included in the java package but Mapred is the older API and mapreduce is the new one. 
	Do not mix them.