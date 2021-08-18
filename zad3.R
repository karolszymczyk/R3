#3.1
rankAccount <- function(description, valueSort, colname, groupName,num){
  fileConnection <- file(description = description ,open="r")
  data <- read.table(fileConnection, header=TRUE, fill=TRUE,sep=",")
  data <- data[order(data[valueSort],decreasing = TRUE),]
  newdataframe <- data[which(data[colname] == groupName),]
  head(newdataframe,num)
}

rankAccount("konta.csv","saldo","occupation", "LISTONOSZ",10)

#3.2
rankAccountBigDatatoChunk <- function(description, size, valueSort, column, groupName, num ){
  fileConnection <- file(description = description ,open="r")
  data <- read.table(fileConnection, nrows=size, header=TRUE, fill=TRUE,sep=",")
  data1 <- data
  columnsNames<- names(data)
  repeat{
    #print(data)
    if(nrow(data)==0){
      close(fileConnection)
      break}
    data <- read.table(fileConnection, nrows=size, col.names = columnsNames, fill=TRUE,sep=",")
    data1 <- rbind(data1,data)
    data1 <- data1[order(data1[valueSort],decreasing = TRUE),]
    data1 <- data1[which(data1[column] == groupName),]
    data1 <- head(data1, num)}
  
  head(data1,num)}

rankAccountBigDatatoChunk("konta.csv", 1000, "saldo", "occupation", "NAUCZYCIEL", 5)

#3.3
library(DBI)
library(RSQLite)

readToBase<-function(filepath,dbpath,tablename,size,sep=",",header=TRUE,delete=TRUE){
  ap<-!delete
  ov<-delete
  fileConnection<- file(description = filepath,open="r")
  dbConn<-dbConnect(SQLite(),dbpath)
  data<-read.table(fileConnection,nrows=size,header = header,fill=TRUE,sep=sep)
  columnsNames<-names(data)
  dbWriteTable(conn = dbConn,name=tablename,data,append=ap,overwrite=ov)
  repeat{
    if(nrow(data)==0){
      close(fileConnection)
      dbDisconnect(dbConn)
      break}
    
    data<-read.table(fileConnection,nrows=size,col.names=columnsNames,fill=TRUE,sep=sep)
    dbWriteTable(conn = dbConn,name=tablename,data,append=TRUE,overwrite=FALSE)}}

readToBase("konta.csv","praca3.SQLite", "praca3",1000)

SQLFunction <- function(tabelaZbazyDanych, colname, Value, valueSort, num){
  dbp <- "praca3.sqlite"
  con <- dbConnect(SQLite(),dbp)
  dbGetQuery(con, paste0("SELECT * FROM ",tabelaZbazyDanych," WHERE ",colname ," = '", Value,"' ORDER BY ",valueSort," DESC limit ",num))}



SQLFunction("praca3", "occupation","NAUCZYCIEL", "saldo", 5)
