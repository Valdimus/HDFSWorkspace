# HDFSWorkspace 0.1.1

* Fix pv on Rstudio, to have the progession of the load/save of the workspace in R console.
* Add message when moving the RData on HDFS to his final place.

# HDFSWorkspace 0.1.0

* Save Workspace on HDFS.
* Load Workspace on HDFS.
* Disable or enable the save of the workspace when closing R (.Last).
* Automatically load or save the current Workspace if no file is giving to the saveWorkspaceHDFS/loadWorkspaceHDFS. It use the working directory to push a .RData file on HDFS like hdfs://namenode:8020/user/USER/.rdirname/WORKING_DIRECTORY/.RData.



