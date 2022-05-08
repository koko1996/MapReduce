# MapReduce system
This repository contains an implmentation of a MapReduce system described in http://nil.csail.mit.edu/6.824/2020/labs/lab-mr.html. It consists of worker processes each of which calls application Map and Reduce functions and handles reading and writing files, and a master process that hands out tasks to workers and copes with failed workers.

### How To Run
The test-mr.sh in src/main runs the system (by spawining a master and 3 worker threads). You can view the contents of that file for an example on how to run the system.