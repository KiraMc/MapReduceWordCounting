#include <omp.h>
#include <iostream>
#include <cstdio>
#include <fstream>
#include <sstream>
#include <string.h>
#include <vector>
#include <map>
#include <dirent.h>
#include <unistd.h>
#include <algorithm>
#include <mpi.h>
#include <stddef.h>
#include "sendFileByFile.h"

using namespace std;

//Number of reader, mapper and reducer
#define NUMREADER 6
#define NUMMAPPER 6
#define NUMREDUCER 4

//Define Tag
#define FILETAG 99
#define TUPLETAG 98

//Queue size
#define QUEUESIZE 20000

//Global locks
omp_lock_t readMapLocks[NUMMAPPER];
omp_lock_t sendLock;
omp_lock_t recvReduceLocks[NUMREDUCER];

//Global Queues
WorkQue readMapQue[NUMMAPPER];
WorkQue sendQue;
WorkQue recvReduceQue[NUMREDUCER];

void fileSendThread(vector<string> fileNames,int numP){
  vector<string>::iterator it = fileNames.begin();
  while(it != fileNames.end()){
    //Blocking receive request
    MPI_Status recvSta;
    int recvFlag;
    
    MPI_Recv(&recvFlag,1,MPI_INT,MPI_ANY_SOURCE,FILETAG,MPI_COMM_WORLD,&recvSta);

    MPI_Send((*it).c_str(),(*it).size()+1,MPI_CHAR,recvSta.MPI_SOURCE,recvFlag,MPI_COMM_WORLD);
    it++;
  }

  for(int fiCount=0;fiCount < numP*NUMREADER;fiCount++){
    //Blocking receive request
    MPI_Status recvSta;
    int recvFlag;
    MPI_Recv(&recvFlag,1,MPI_INT,MPI_ANY_SOURCE,FILETAG,MPI_COMM_WORLD,&recvSta);

    //blocking send finish signal
    char finished[5] = "NULL";
    MPI_Send(finished,5,MPI_CHAR,recvSta.MPI_SOURCE,recvFlag,MPI_COMM_WORLD);
  }
  printf("File sender finished\n");
}

void readerThread(int rank,int id){
  //Keep reading files until NULL received
  while(1){
    //Blocking send request
    int sendFlag = id;
    MPI_Send(&sendFlag,1,MPI_INT,0,FILETAG,MPI_COMM_WORLD);

    //Blocking receive file name
    MPI_Status recvSta;
    char fileName[30];
    MPI_Recv(fileName,30,MPI_CHAR,0,id,MPI_COMM_WORLD,&recvSta);
    if(strcmp(fileName,"NULL") == 0){
      break;
    }else{
      //ifstream inFile;
      printf("Rank %d: Reader %d start to read file %s\n",rank,id,fileName);
      ifstream inFile;
      inFile.open(fileName);
      if(!inFile){
	continue;
      }
      string word;
      while(inFile >> word){
	for(string::iterator it=word.begin();it <  word.end();it++){
	  if((*it) != '\'' && ((*it)<65 || (*it)>122)) word.erase(word.find_first_of(*it));
	}
	if(word == "") continue;
	transform(word.begin(),word.end(),word.begin(),::tolower);
	WordTuple data;
	strcpy(data.word,word.c_str());
	data.count = 1;
	data.read = 0;
	int sucFlag = -1;
	int queId = id%NUMMAPPER;
	while(sucFlag != 0){
	  omp_set_lock(&readMapLocks[queId]);
	  sucFlag = readMapQue[queId].writeQue(data);
	  omp_unset_lock(&readMapLocks[queId]);
	  if(sucFlag == 0) break;
	  printf("Rank %d: Reader %d fail to write to full %d\n",rank,id,queId);
	  usleep(5000);
	}
      }
      inFile.close();
      //Put finish for current file info on queue
      WordTuple finishedFile;
      finishedFile.count = -1;
      strcpy(finishedFile.word,"Finished");
      finishedFile.read = 0;

      int sucFlag = -1;
      int queId = id%NUMMAPPER;
      while(sucFlag != 0){
	omp_set_lock(&readMapLocks[queId]);
	sucFlag = readMapQue[queId].writeQue(finishedFile);
	omp_unset_lock(&readMapLocks[queId]);
	if(sucFlag == 0) break;
	printf("Rank %d: Reader %d fail to write finished to full %d\n",rank,id,queId);
	usleep(5000);
      }
    }
  }

  //Put overall finished info on queue
  WordTuple finishedAll;
  finishedAll.count = -2;
  strcpy(finishedAll.word, "Finished");
  finishedAll.read = 0;
  printf("Rank %d: Reader %d finished\n",rank,id);
  int sucFlag = -1;
  int queId = id%NUMMAPPER;
  while(sucFlag != 0){
    omp_set_lock(&readMapLocks[queId]);
    sucFlag = readMapQue[queId].writeQue(finishedAll);
    omp_unset_lock(&readMapLocks[queId]);
    if(sucFlag == 0) break;
    printf("Rank %d: Reader %d fail to write finished to full %d\n",rank,id,queId);
    usleep(5000);
  }
}

void mapperThread(int rank, int id){
  //Loop to wait for overall finish
  while(1){
    map<string,int> wordCount;
    int terminate = 0;
    //Loop to wait for 1 file finish
    while(1){
      WordTuple data;
      int queId = id%NUMMAPPER;
      //Grab data from queue
      while(1){
	omp_set_lock(&readMapLocks[queId]);
	int sucFlag = readMapQue[queId].readQue(&data);
	omp_unset_lock(&readMapLocks[queId]);

	if(sucFlag == 0)break;
	printf("Rank %d: Mapper %d fail to read empty %d\n",rank,id,queId);
	usleep(20000);
      }
      
      //Check whether it's a finish or not
      if(data.count == -2){
	terminate = 1;
	break;
      }
      if(data.count == -1)	break;

      //Insert key or increase count in wordCount
      map<string,int>::iterator it = wordCount.find(data.word);
      if(it == wordCount.end()){
	wordCount.insert(pair<string,int>(data.word,data.count));
      }else{
	it->second += data.count;
      }
    }

    //Break if detect overall finish
    if(terminate != 0) break;

    //Put records on sending queue;
    for(map<string,int>::iterator it = wordCount.begin();it != wordCount.end();it++){
      WordTuple data;
      strcpy(data.word,(it->first).c_str());
      data.count = it->second;
      while(1){
	omp_set_lock(&sendLock);
	int sucFlag = sendQue.writeQue(data);
	omp_unset_lock(&sendLock);

	if(!sucFlag) break;
	printf("Rank %d: Mapper %d fail to write to full sendQue\n",rank,id);
	usleep(10000);
      }
    }
  }
  printf("Rank %d: Mapper %d finished\n",rank,id);
  //Put finish info on send queue
  WordTuple finished;
  finished.count = -1;
  strcpy(finished.word, "Finished");
  finished.read = 0;
  while(1){
    omp_set_lock(&sendLock);
    int sucFlag = sendQue.writeQue(finished);
    omp_unset_lock(&sendLock);

    if(!sucFlag) break;
    printf("Rank %d: Mapper %d fail to write finish to full sendQue\n",rank,id);
    usleep(10000);
  }
}

void senderThread(int rank,int numP){
  int finishedMappers = 0;
  double sendTime = 0;
  //Create new MPI type for word tuple
  int count = 3; //Number of types
  int blockLengths[3] = {1,sizeof(char)*30,1};
  MPI_Aint displArray[3] = {offsetof(WordTuple,count),offsetof(WordTuple,word),offsetof(WordTuple,read)};
  MPI_Datatype typeArray[3] = {MPI_INT,MPI_CHAR,MPI_INT};
  MPI_Datatype tempType,tupleType;
  MPI_Aint lb,extent;

  MPI_Type_create_struct(count,blockLengths,displArray,typeArray,&tempType);
  MPI_Type_get_extent(tempType,&lb,&extent);
  MPI_Type_create_resized(tempType,lb,extent,&tupleType);
  MPI_Type_commit(&tupleType);

  while(1){
    WordTuple data;
    while(1){
      omp_set_lock(&sendLock);
      int sucFlag = sendQue.readQue(&data);
      omp_unset_lock(&sendLock);

      if(!sucFlag) break;
      printf("Rank %d: Fail to read from empty sendQue\n",rank);
      usleep(5000);
    }

    //Record number of finished mappers
    if(data.count == -1){
      finishedMappers++;
      //Break if all mappers finished
      if(finishedMappers == NUMMAPPER) break;
      else continue;
    }
    
    
    //Send the tuple out--------------------------------------
    //Use really simple hashing to get dest rank
    double startTime = MPI_Wtime();
    unsigned hashval = 0;
    for(int i = 0;i<strlen(data.word);i++){
      hashval += data.word[i];
    }
    
    int destRank = hashval % numP;
    
    //Send out
    MPI_Request sendReqs;
    MPI_Send(&data,1,tupleType,destRank,TUPLETAG,MPI_COMM_WORLD);
    sendTime += MPI_Wtime()-startTime;
  }

  printf("Rank %d: sender finished with send time %lf\n",rank,sendTime);
  //Send finish process finished info to all process
  WordTuple finished;
  strcpy(finished.word,"Finished");
  finished.count = -1;
  finished.read = 0;
  for(int i=0;i<numP;i++){
    MPI_Send(&finished,1,tupleType,i,TUPLETAG,MPI_COMM_WORLD);
  }
}

void receiverThread(int rank,int numP){
  int finishedPro = 0;
  double receiveTime = 0;
  //create new struct
  int count = 3; //Number of types
  int blockLengths[3] = {1,sizeof(char)*30,1};
  MPI_Aint displArray[3] = {offsetof(WordTuple,count),offsetof(WordTuple,word),offsetof(WordTuple,read)};
  MPI_Datatype typeArray[3] = {MPI_INT,MPI_CHAR,MPI_INT};
  MPI_Datatype tempType,tupleType;
  MPI_Aint lb,extent;

  MPI_Type_create_struct(count,blockLengths,displArray,typeArray,&tempType);
  MPI_Type_get_extent(tempType,&lb,&extent);
  MPI_Type_create_resized(tempType,lb,extent,&tupleType);
  MPI_Type_commit(&tupleType);

  while(1){
    double startTime = MPI_Wtime();
    MPI_Status status;
    WordTuple data;
    //Receive data tuple
    MPI_Recv(&data,1,tupleType,MPI_ANY_SOURCE,TUPLETAG,MPI_COMM_WORLD,&status);
    receiveTime += MPI_Wtime()-startTime;
    //Record number of finished process
    if(data.count == -1){
      finishedPro ++;
      if(finishedPro == numP) break;
      else continue;
    }

    //Use complicated hash to get more presice hash value
    unsigned hashval = 3;
    for(int i = 0;i<strlen(data.word);i++){
      hashval = hashval*3*data.word[i]*11;
    }
    
    int queId = hashval % NUMREDUCER;

    //Place tuple on receiveReduce queue
    while(1){
      omp_set_lock(&recvReduceLocks[queId]);
      int sucFlag = recvReduceQue[queId].writeQue(data);
      omp_unset_lock(&recvReduceLocks[queId]);

      if(!sucFlag) break;
      printf("Rank %d: Fail to write to full reduce queue %d\n",rank,queId);
      usleep(5000);
    }
  }
  
  printf("Rank %d: receiver finished with receive time %lf\n",rank,receiveTime);
  //Place finish info on reduce queue
  WordTuple finished;
  strcpy(finished.word,"Finished");
  finished.count = -1;
  finished.read = 0;
  for(int i =0;i<NUMREDUCER;i++){
    while(1){
      omp_set_lock(&recvReduceLocks[i]);
      int sucFlag = recvReduceQue[i].writeQue(finished);
      omp_unset_lock(&recvReduceLocks[i]);

      if(!sucFlag) break;
      printf("Rank %d: Fail to write finish to full reduce queue %d\n",rank,i);
      usleep(5000);
    }
  }
}

void reducerThread(int rank,int id){
  map<string,int> wordCount;
  
  while(1){
    WordTuple data;
    while(1){
      omp_set_lock(&recvReduceLocks[id]);
      int sucFlag = recvReduceQue[id].readQue(&data);
      omp_unset_lock(&recvReduceLocks[id]);

      if(sucFlag == 0) break;
      printf("Rank %d: Reducer %d fail to read empty %d\n",rank,id,id);
      usleep(20000);
    }

    if(data.count == -1) break;
    map<string,int>::iterator it = wordCount.find(data.word);
    if(it == wordCount.end()){
      wordCount.insert(pair<string,int>(data.word,data.count));
    }else{
      it->second += data.count;
    }
  }
  printf("Rank %d: reducer %d finished\n",rank,id);
  //Write to file
  ofstream outputFile;
  stringstream idString;
  stringstream rankString;
  idString << id;
  rankString << rank;
  string fileName = "Rank_"+rankString.str()+"_Thread_"+idString.str()+".txt";
  outputFile.open(fileName.c_str());

  for(map<string,int>::iterator it = wordCount.begin();it != wordCount.end();it++){
    stringstream countString;
    countString << it->second;
    string outLine = it->first+" "+countString.str()+"\n";
    outputFile << outLine;
  }

  outputFile.close();
}

int main(int argc,char* argv[]){
  int numP,rank,provided;
  double timeCost;
  
  if(NUMREADER != NUMMAPPER) {
    printf("Usage: number of reader has to be equal to number of mapper\n");
    return 0;
  }

  MPI_Init_thread(&argc,&argv,MPI_THREAD_MULTIPLE,&provided);
  MPI_Comm_size(MPI_COMM_WORLD, &numP);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  if(rank == 0) timeCost = MPI_Wtime();

  //Init locks
  for(int i=0;i<NUMMAPPER;i++){
    omp_init_lock(&readMapLocks[i]);
  }
  for(int i=0;i<NUMREDUCER;i++){
    omp_init_lock(&recvReduceLocks[i]);
  }
  omp_init_lock(&sendLock);

  if(rank == 0) timeCost = MPI_Wtime();
  vector<string> fileNames;
  //Get all the filenames in rank 0
  if(rank == 0){
    DIR* dirD;
    struct dirent *edir;

    dirD = opendir("./RawText/");
    if(dirD == NULL){
      cout<<"Fail to open dir"<<endl;
      return -1;
    }
    while(edir = readdir(dirD)){
      if(strcmp(edir->d_name,".") && strcmp(edir->d_name,"..")){
	stringstream ss;
	string filePath;
	ss << edir->d_name;
	ss >> filePath;
	filePath = "./RawText/"+filePath;
	//printf("%s\n",filePath.c_str());
	fileNames.push_back(filePath);
	//fileNames.push_back(edir->d_name);
      }
    }
    closedir(dirD);
  }

  omp_set_num_threads(NUMREADER+NUMMAPPER+NUMREDUCER+3);
  #pragma omp parallel
  {
    //File allocation thread only in rank 0
    #pragma omp single nowait
    {
      if(rank == 0){
	#pragma omp task untied
	fileSendThread(fileNames,numP);
      }
    }

    //Reader threads
    #pragma omp single nowait
    {
	for(int i=0;i<NUMREADER;i++){
          #pragma omp task untied
	  readerThread(rank,i);
	}
    }

    //Mapper threads
    #pragma omp single nowait
    {
      for(int i=0;i<NUMMAPPER;i++){
	#pragma omp task untied
	mapperThread(rank,i);
      }
    }

    //Sender thread
    #pragma omp single nowait
    {
      #pragma omp task untied
      senderThread(rank,numP);
    }

    //Receiver thread
    #pragma omp single nowait
    {
      #pragma omp task untied
      receiverThread(rank,numP);
    }

    //Reducer threads
    #pragma omp single nowait
    {
	for(int i=0;i<NUMREDUCER;i++){
          #pragma omp task untied
	  reducerThread(rank,i);
	}
    }
  }
  
  if(rank == 0) printf("Time cost for mpi couting is %lf\n",MPI_Wtime()-timeCost);
  MPI_Finalize();
}
