#pragma once

struct WordTuple{
 public:
  int count;
  char word[30];
  int read;
};

class WorkQue{
 private:
  int readPtr;
  int writePtr;
  WordTuple data[QUEUESIZE];
 public:
  int isQueEmpty();
  int isQueFull();
  int readQue(WordTuple* data);
  int writeQue(WordTuple data);
  void printQue();
  WorkQue(){
    readPtr = 0;
    writePtr = 0;
    for(int i=0;i<QUEUESIZE;i++){
      data[i].read = 1;
    }
  }
};
void WorkQue::printQue(){
  for(int i=0;i<QUEUESIZE;i++){
    printf("%d %s %d\n",i,this->data[i].word,this->data[i].count);
  }
}

int WorkQue::isQueEmpty(){
  if(this->readPtr == this->writePtr && this->data[this->readPtr].read == 1) return 1;
  return 0;
}
int WorkQue::isQueFull(){
  if(this->readPtr == this->writePtr && this->data[this->writePtr].read == 0) return 1;
  return 0;
}
int WorkQue::readQue(WordTuple* data){
  if(this->isQueEmpty()){
    //cerr<<"Read from empty queue fail."<<endl;
    return -1;
  }
  this->data[this->readPtr].read = 1;
  *data = this->data[this->readPtr];
  this->readPtr = (this->readPtr + 1)%QUEUESIZE;
  return 0;
}
int WorkQue::writeQue(WordTuple data){
  if(this->isQueFull()){
    //cerr<<"Write to full queue fail."<<endl;
    return -1;
  }
  data.read = 0;
  this->data[this->writePtr] = data;
  
  this->writePtr = ((this->writePtr)+1)%QUEUESIZE;
  return 0;
}