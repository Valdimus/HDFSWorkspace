# Author: Christophe NOUCHET
# Email: nouchet.christophe@gmail.com
# Date: 27/08/2017
# How To Use: You must set HADOOP_HOME and HADOOP_BIN in your Renviron.site

##' @name hadoop_command
##' @description Get the bin path of Hadoop, HADOOP_HOME and HADOOP_BIN must be set in your Renviron.site
##' @param command The hadoop command to get the absolute path of
##' @return The absolute path of the Hadoop command
hadoop_command <- function(command="") {
  hadoop_bin <- Sys.getenv("HADOOP_BIN")
  if(hadoop_bin == ""){
    hadoop_bin <- "/usr/bin"
  }
  return(fix_path(file.path(hadoop_bin, command)))
}

##' @name hdfs_command
##' @description Get the absolute path of the hdfs command
##' @param options Option to git to the hdfs command
##' @return The absolute path of the hdfs command
hdfs_command <- function(options="") {
  return(hadoop_command(paste("hdfs", options, sep=" ")))
}

##' @name hdfs_dfs_command
##' @description Get the absolute path of the hdfs command
##' @param options Option to git to the dfs command
##' @return The absolute path of the dfs command
hdfs_dfs_command <- function(options) {
  return(hdfs_command(paste("dfs", options, sep=" ")))
}

##' @name hdfs_dfs_ls
##' @description Liste file on hadoop, get file size
##' @param options Options to git to ls
##' @return List of file
hdfs_ls <- function(options="") {
  # Execute hdfs command
  raw_file <- system(command = hdfs_dfs_command(paste("-ls", options, "|grep -v \"Found .*item.*\" | awk '{print $8, $5}'", sep=" ")), intern=TRUE)

  # List of file
  liste <- list()

  # Get the file and their size
  for(i in raw_file){
    temp_list <- strsplit(i, " ")
    liste[[temp_list[[1]][1]]] <- temp_list[[1]][2]
  }
  return(liste)
}

##' @name readHDFSFile
##' @description Get a file on HDFS
##' @param filename The filename on HDFS to load
##' @return file
readHDFSFile <- function(filename, options="rb") {

  # Check if the Rdata exist on HDFS
  hdfs_files <- hdfs_ls(filename)
  if(filename %in% names(hdfs_files)){
    message("Read file '", filename, "' on HDFS")

    # Get the file size
    file_size <- hdfs_files[[filename]]

    # Use pv to have the progress of the loading
    connection <- pipe(paste(hdfs_dfs_command(paste("-cat",filename, sep=" ")),"| pv --size",file_size, sep=" "), options)

    return(connection)
  }
  else{
    message("File '", filename, "' not found on HDFS")
    return(FALSE)
  }
}

##' @name writeHDFSFile
##' @description Save a file on HDFS
##' @param filename The filename on HDFS to save
##' @return file
writeHDFSFile <- function(filename, options="wb") {
  connection <- pipe(paste("pv |", hdfs_dfs_command("-put -"),filename, sep=" "), options)
  return(connection)
}


##' @name hdfs_mkdir
##' @param path Path to the directory to create
hdfs_mkdir <- function(path) {
  suppressWarnings(system(hdfs_dfs_command(paste("-mkdir", path)), ignore.stderr = TRUE, ignore.stdout = TRUE, intern = TRUE))
}
