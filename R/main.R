# Author: Christophe NOUCHET
# Email: nouchet.christophe@gmail.com
# Date: 27/08/2017
# Infomation: Thanks to Nicolas Dupeux for the POC that inspired this package, especially for the use of pv command
# Documentation: This package is a simple way to save user workspace on HDFS instead of local disk. It's necessary if you have multiple R server than share their home on
#                nfs (or something else) and you don't have that much free space.


#' @title A little fix for file.path
#' @name fix_path
#' @description  Ok, file.path don't check if the path already have a '/' at the start or end of parameters, so you can have double "/" ...
fix_path <- function(path) {
  return(gsub("///", "/", gsub("//", "/", path)))
}


#' @title Get the R directory to use on HDFS
#' @name getRDirName
#' @description Get the path to use for creating HDFS tree
#' @return string
getRDirName <- function() {
  rdirname <- Sys.getenv("HADOOP_RDIRNAME")
  if(rdirname == ""){
    rdirname <- ".rdirname"
  }
  return(rdirname)
}

#' @title Don't save Workspace on HDFS
#' @name setSavedWorkspace
#' @description Don't save Workspace on HDFS
#' @export
setSavedWorkspace <- function(action="yes") {
  Sys.setenv(HDFSWorkspace_SAVE=action)
}

#' @title Know if we have to save Workspace
#' @name getSavedWorkspace
#' @description Know if we have to save Workspace
#' @export
getSavedWorkspace <- function() {
  return(substr(Sys.getenv("HDFSWorkspace_SAVE"), 0, 1) == "y")
}

#' @title Get the project path on HDFS
#' @name getProjectPath
#' @description Get the project path
#' @return string
getProjectPath <- function() {

  return(fix_path(file.path(getRDirName(), getwd())))
}

#' @title Load the workspace from HDFS
#' @name loadWorkspaceHDFS
#' @description Load Workspace from HDFS
#' @param filename The filename to load on HDFS. If null, filname will be getwd() + "/.RData".
#' @return boolean
#' @export
loadWorkspaceHDFS <- function(filename = NULL) {
  setSavedWorkspace(action="yes")

  # Auto name
  if(is.null(filename) && file.exists(fix_path(file.path(getwd(), ".RDataHDFS")))) {
    filename <- fix_path(file.path(getProjectPath(), ".RData"))
  }
  else {
    return(FALSE)
  }

  # Get the file on HDFS
  connection <- readHDFSFile(filename = filename)

  if(connection == FALSE){
    message("No Workspace load from HDFS")
    return(FALSE)
  }

  message("Load Workspace from HDFS: ", filename)

  # Read it and populate user workspace
  load(envir=.GlobalEnv, file = connection)
  close(connection)

  message("Load Workspace from HDFS: OK")

  return(TRUE)
}

#' @title Save Workspace on HDFS
#' @name saveWorkspaceHDFS
#' @description Save your workspace on HDFS
#' @param filename Path to the file on HDFS. If null, filname will be getwd() + "/.RData".
#' @return boolean
#' @export
saveWorkspaceHDFS <- function(filename = NULL) {

  if(!getSavedWorkspace()){
    return(FALSE)
  }

  # Auto name
  if(is.null(filename)) {
    project_path <- getProjectPath()

    #Create the directory
    hdfs_mkdir(project_path)

    # SaveWorkspace
    filename <- fix_path(file.path(project_path, ".RData"))
  }

  message("Save Workspace on HDFS: ", filename)

  # We write the file on a temp_file for more safety (if the write failed, you don't loose the previous one)
  tmp_filename = paste(filename, as.numeric(as.POSIXlt(Sys.time())), sep="-")

  # Write file on HDFS
  connection <- writeHDFSFile(tmp_filename)
  save(list = ls(envir=.GlobalEnv, all.names = TRUE), file = connection)
  close(connection)

  # Delete old file and move the new one
  message("Move the .Rdata to his final place on HDFS: [1/2]")
  suppressWarnings(system(hdfs_dfs_command(paste("-rm", filename)), ignore.stderr = TRUE, ignore.stdout = TRUE, intern = TRUE))
  message("Move the .Rdata to his final place on HDFS: [2/2]")
  suppressWarnings(system(hdfs_dfs_command(paste("-mv", tmp_filename, filename)), intern=TRUE, ignore.stderr = TRUE, ignore.stdout = TRUE))

  message("Save Workspace on HDFS: OK")

  file.create(".RDataHDFS")

  # Clean user workspace as we want that R only save it on HDFS
  rm(list=ls(envir=globalenv(), all.names = TRUE), envir = globalenv())

  return(TRUE)
}
