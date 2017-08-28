# Author: Christophe NOUCHET
# Email: nouchet.christophe@gmail.com
# Date: 27/08/2017
# Infomation: Thanks to Nicolas Dupeux for the POC that inspired this package, especially for the use of pv command
# Documentation: Some primitive to use HDFS. All this function use "system" to call hdfs command. To make this possible, you must set on your Renviron.site:
#                - HADOOP_HOME
#                - JAVA_HOME
#                - HADOOP_BIN

#' @title Get the absolute path to hadoop bin directory
#' @name hadoop_command
#' @description Get the bin path of Hadoop, HADOOP_HOME and HADOOP_BIN must be set in your Renviron.site
#' @param command The hadoop command to get the absolute path of
#' @return The absolute path of the Hadoop command
hadoop_command <- function(command="") {
  hadoop_bin <- Sys.getenv("HADOOP_BIN")
  if(hadoop_bin == ""){
    hadoop_bin <- "/usr/bin"
  }
  return(fix_path(file.path(hadoop_bin, command)))
}

#' @title Get the absolute path to 'hdfs' command
#' @name hdfs_command
#' @description Get the absolute path of the hdfs command
#' @param options Option to git to the hdfs command
#' @return The absolute path of the hdfs command
hdfs_command <- function(options="") {
  return(hadoop_command(paste("hdfs", options, sep=" ")))
}

#' @title Get the absolute path to 'hdfs dfs' command
#' @name hdfs_dfs_command
#' @description Get the absolute path of the hdfs command
#' @param options Option to git to the dfs command
#' @return The absolute path of the dfs command
hdfs_dfs_command <- function(options) {
  return(hdfs_command(paste("dfs", options, sep=" ")))
}

#' @title Liste file/directory on HDFS
#' @name hdfs_dfs_ls
#' @description Liste file on hadoop, get file size
#' @param options Options to git to ls
#' @return List of files/directories with their size
hdfs_ls <- function(options="") {
  # Execute hdfs command
  suppressWarnings(raw_file <- system(command = hdfs_dfs_command(paste("-ls", options, "|grep -v \"Found .*item.*\" | awk '{print $8, $5}'", sep=" ")), intern=TRUE, ignore.stderr = TRUE))

  # List of file
  liste <- list()

  # Get the file and their size
  for(i in raw_file){
    temp_list <- strsplit(i, " ")
    liste[[temp_list[[1]][1]]] <- temp_list[[1]][2]
  }
  return(liste)
}

#' @title Read a file from HDFS
#' @name readHDFSFile
#' @description Get a file on HDFS
#' @param filename The filename on HDFS to load
#' @param options File mode
#' @return A file object that you can use with load
readHDFSFile <- function(filename, options="rb") {

  # Check if the Rdata exist on HDFS
  hdfs_files <- hdfs_ls(filename)
  if(filename %in% names(hdfs_files)){
    message("Read file '", filename, "' on HDFS")

    # Get the file size
    file_size <- hdfs_files[[filename]]

    # Use pv to have the progress of the loading
    connection <- pipe(paste(hdfs_dfs_command(paste("-cat",filename, sep=" ")),"| pv --format '%N %b %t %r %p %e\n' --force --size",file_size, sep=" "), options)

    return(connection)
  }
  else{
    #message("File '", filename, "' not found on HDFS")
    return(FALSE)
  }
}

#' @title Write a file on HDFS
#' @name writeHDFSFile
#' @description Save a file on HDFS
#' @param filename The filename on HDFS to save
#' @param options File mode
#' @return A file object that you can use with save
writeHDFSFile <- function(filename, options="wb") {
  connection <- pipe(paste("pv --format '%N %b %t %r %p %e\n' --force |", hdfs_dfs_command("-put -"),filename, sep=" "), options)
  return(connection)
}


#' @title Make directories on HDFS
#' @name hdfs_mkdir
#' @param path Path to the directory to create
hdfs_mkdir <- function(path, debug = TRUE) {
  suppressWarnings(system(hdfs_dfs_command(paste("-mkdir", path)), ignore.stderr = debug, ignore.stdout = debug, intern = debug))
}
