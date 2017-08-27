# Author: Christophe NOUCHET
# Email: nouchet.christophe@gmail.com
# Date: 27/08/2017
# How To Use: You must set HADOOP_HOME and HADOOP_BIN in your Renviron.site

ELEMENT_TO_RM_FROM_ENV <- c("hadoop_command", "hdfs_command", "hdfs_dfs_command", "hdfs_ls", "readHDFSFile",
                            "writeHDFSFile", "loadWorkspaceHDFS", "saveWorkspaceHDFS", "ELEMENT_TO_RM_FROM_ENV",
                            "hdfs_mkdir", "getRDirName", "getProjectPath", "autoSaveWorkspaceHDFS", "autoLoadWorkspace", "fix_path")

##' @name fix_path
##' @description  Ok, file.path don't check if the path already have a '/' at the begining or end of parameters, so you can have double "/" ...
fix_path <- function(path) {
  return(gsub("///", "/", gsub("//", "/", path)))
}

getRDirName <- function() {
  rdirname <- Sys.getenv("HADOOP_RDIRNAME")
  if(rdirname == ""){
    rdirname <- ".rdirname"
  }
  return(rdirname)
}

##' @name loadWorkspaceHDFS
##' @description Load Workspace from HDFS
##' @param filename The filename to load on HDFS
##' @return boolean
loadWorkspaceHDFS <- function(filename) {
  connection <- readHDFSFile(filename = filename)
  if(connection == FALSE){
    message("No Rdata found on HDFS")
    return(FALSE)
  }
  message("Load Workspace from HDFS: ", filename)
  load(envir=.GlobalEnv, file = connection)
  message("Load Workspace from HDFS: OK")
  close(connection)
  return(TRUE)

}

##' @name saveWorkspaceHDFS
##' @description Save your workspace on HDFS
##' @param filename Path to the file on HDFS
##' @return boolean
saveWorkspaceHDFS <- function(filename) {
  message("Save Workspace on HDFS: ", filename)
  tmp_filename = paste(filename, as.numeric(as.POSIXlt(Sys.time())), sep="-")
  connection <- writeHDFSFile(tmp_filename)
  myenv <- ls(envir=.GlobalEnv, all.names = TRUE)

  for(element in ELEMENT_TO_RM_FROM_ENV)
  {
    myenv <- myenv[myenv != element]
  }

  save(list = myenv, file = connection)

  close(connection)
  suppressWarnings(system(hdfs_dfs_command(paste("-rm", filename)), ignore.stderr = TRUE, ignore.stdout = TRUE, intern = TRUE))
  suppressWarnings(system(hdfs_dfs_command(paste("-mv", tmp_filename, filename)), intern=TRUE, ignore.stderr = TRUE, ignore.stdout = TRUE))
  message("Save Workspace on HDFS: OK")
  rm(list=ls(envir=globalenv(), all.names = TRUE), envir = globalenv())
  return(TRUE)
}


##' @name getProjectPath
##' @description Get the project path
##' @return string
getProjectPath <- function() {

  return(fix_path(file.path(getRDirName(), getwd())))
}

##' @name autoSaveWorkspaceHDFS
##' @description Automaticely save the workspace on HDFS
##' @return boolean
autoSaveWorkspaceHDFS <- function()
{
  project_path <- getProjectPath()

  #Create the directory
  hdfs_mkdir(project_path)

  # SaveWorkspace
  return(saveWorkspaceHDFS(fix_path(file.path(project_path, ".Rdata"))))
}

##' @name autoLoadWorkspace
##' @description Automaticely load the workspace from HDFS
autoLoadWorkspace <- function() {
  project_path <- fix_path(file.path(getProjectPath(), ".Rdata"))
  return(loadWorkspaceHDFS(project_path))
}
