# HDFSWorkspace

## Description

If you don't have that much amount of free space on your local disk but you have an Hadoop cluster not far away, you can save your R Workspace on HDFS.

This is very useful if you have shared server that share home's directories (with NFS for example) and user that use a large amount of RAM (> 10Go).

## How to use it

### Set variable environment

You have to set some environment variable for Hadoop:

* HADOOP_BIN: Where hadoop binaries are
* JAVA_HOME: Java home
* HADOOP_HOME: Hadoop home

In file /etc/R/Renviron.site:
```
HADOOP_BIN=/opt/hadoop-3.0.0-alpha4/bin
JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
HADOOP_HOME=/opt/hadoop-3.0.0-alpha4
```

### Automatic save and restore of workspace

To autotmaticely save and restore workspace that is on HDFS, you have to completed the .First and .Last function:

For example in file /etc/R/Rprofile.site:

```R
.First <- function() {
        if(interactive()) {
                require("HDFSWorkspace", quietly = TRUE)
                loadWorkspaceHDFS()
        }
}

.Last <- function() {
        if(interactive()) {
                require("HDFSWorkspace", quietly = TRUE)
                saveWorkspaceHDFS()
        }
}
```

# TODO

* Handle the quit(save="no"), at this time, it always save workspace. As workaround, I had saveWorkspace function to set the choosen behavior.

