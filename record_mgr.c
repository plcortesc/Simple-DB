#include "storage_mgr.h"
#include "record_scan.h"
//new
#include "buffer_mgr.h"
#include "record_mgr.h"


#include <stdlib.h>
#include <stdio.h>
#include <string.h>

//linked list
static Scan_Node_ptr RM_Scan_stptr=NULL;
//global schema
static Schema gSchema;
//to share strings from DB
static char datatext[];

//this method returns the total 
//size of a record 
int getRecordSize(Schema *schema) {
  //used for the DT_STRING to let us 
  //know which is the string size
  int *typeLength = schema->typeLength;
  int size = 0;
  int i;
  for (i = 0; i < schema->numAttr; i++) {
      //checking datatypes to sum the total size
      switch(schema->dataTypes[i]){
      case DT_INT: size += sizeof(int); break;
      case DT_FLOAT: size += sizeof(float); break;
      case DT_BOOL: size += sizeof(bool); break;
      case DT_STRING: size += typeLength[i]; break;
    }
  }
  return size+schema->numAttr;
}

//the method creates a new schema
Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes,
                     int *typeLength, int keySize, int *keys) {
  //creating schema 
  Schema *schema = (Schema*) malloc(sizeof(Schema));
  //assigning values
  schema->numAttr = numAttr;
  schema->attrNames = attrNames;
  schema->dataTypes = dataTypes;
  schema->typeLength = typeLength;
  schema->keySize = keySize;
  schema->keyAttrs = keys; 
  
  return schema;
}

//this method cleans the space 
//occupied by the schema
RC freeSchema(Schema *schema) {
  
    free(schema);

    return (RC_OK);
}

//the method creates a new record
RC createRecord(Record **record, Schema *schema) {
  //creating the record
  Record *rec= (Record*) malloc(sizeof(Record));
  //geting the size of the record
  int size= getRecordSize(schema);
  //allocating memory size
  rec->data= (char*) malloc(size);
  //filing rec-> data with 0
  memset(rec->data, 0, size);
  *record= rec;

  return (RC_OK);
}

//this method cleans the space 
//occupied by the record
RC freeRecord(Record *record) {
  free(record);
  return RC_OK;
}

//the method deals with attributes, it is used to get an attribute
RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {
  DataType *dataType = schema->dataTypes;
  int *typeLength = schema->typeLength;
  char *data = record->data;
      char strData[101]={'\0'};

  //creating pointer to the value and allocating in memory
  Value *val = (Value*) malloc(sizeof(Value));
  //temp is used to convert datatypes
  char *temp=NULL;
  int offset=0;
  int i;

  //calculating the value of the offset
  for (i = 0; i < attrNum; i++) {
    switch(dataType[i]){
      case DT_INT: offset += sizeof(int); break;
      case DT_FLOAT: offset += sizeof(float); break;
      case DT_BOOL: offset += sizeof(bool); break;
      case DT_STRING: offset += typeLength[i]; break;
    }
  }
  //updating the offset
  offset+=(attrNum+1);
  int size = 0;
  //depending on datatype we set the value
  switch(dataType[i]){
    case DT_INT:
      //seting value datatype
      val->dt = DT_INT;
      size += sizeof(int);
      temp=malloc(size+1);
      strncpy(temp, data + offset, sizeof(int)); 
      temp[sizeof(int)]='\0';
      //temp being converted to int
      val->v.intV = atoi(temp);
      break;
    case DT_FLOAT:
      //seting value datatype
      val->dt = DT_FLOAT;
            size += sizeof(float);

      temp=malloc(size+1);
      strncpy(temp, data + offset, sizeof(float)); 
      temp[sizeof(float)]='\0';
      //temp being converted to float 
      val->v.floatV = (float) *temp;
      break;
    case DT_BOOL:
      //seting value datatype
      val->dt = DT_BOOL;
            size += sizeof(bool);

      temp=malloc(size+1);
      strncpy(temp, data + offset, sizeof(bool)); 
          temp[sizeof(bool)]='\0';

      //temp being converted to boolean
      val->v.boolV = (bool) *temp;
      break;
    case DT_STRING:
      //seting value datatype
      val->dt = DT_STRING;
      int size = typeLength[i];
      temp=malloc(size+1);
      while(i <= size)
            {
                temp[i]='\0';
                i++;
            }
      strncpy(temp, data + offset, size); 
          temp[size]='\0';

      i=0;
        while(i < size)
        {
            strData[i]=temp[i];
            i++;
        }
      val->v.stringV=temp;
      break;
  }
  *value= val;
  return RC_OK;

}


//the method deals with attributes, it is used to set an attribute
RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) {
  int numAttr = schema->numAttr,j,temp;
  DataType *dtP = schema->dataTypes;
  int *typeLength = schema->typeLength;
  //char *data = record->data;

  int offset = 1;
  if (attrNum < numAttr) {
    int i;
    for (i = 0; i <attrNum; i++) {
      int dt = *(dtP + i);
      if (dt == DT_INT) {
        offset += sizeof(int);
      } else if (dt == DT_FLOAT) {
        offset += sizeof(float);
      } else if (dt == DT_BOOL) {
        offset += sizeof(bool);
      } else if (dt == DT_STRING) { /*In case it is a String, the size will be equal to the corresponding type length*/
        offset += *(typeLength + i);
      }
    }
    offset+=attrNum;

    /*Since this is a pointer, modifying the local *data will in turn modify the particular record*/
        char *addrToModify ;
    if(attrNum==0)
        {
            addrToModify =record->data ;
            addrToModify[0]='|';//Used to sepearate different tuples in a page file.
            addrToModify++;
        }
        else
        {
            addrToModify=record->data+offset;
            (addrToModify-1)[0]=',';//Comma seperator for records in a tuple.
        }


    int dt = value->dt;
    /*Set the datatype of Value*/
    if (dt == DT_INT) {
      sprintf(addrToModify,"%d",value->v.intV);
      while(strlen(addrToModify)!=sizeof(int))
            {
                strcat(addrToModify,"0");
            }
//format integer to string
    for (i=0,j=strlen(addrToModify)-1 ; i < j;i++,j--)
    {
        temp=addrToModify[i];
        addrToModify[i]=addrToModify[j];
        addrToModify[j]=temp;
    }


    } else if (dt == DT_FLOAT) {
      sprintf(addrToModify,"%f",value->v.floatV);
      while(strlen(addrToModify)!=sizeof(float))
            {
                strcat(addrToModify,"0");
            }
//format float to string
    for (i=0,j=strlen(addrToModify)-1 ; i < j;i++,j--)
    {
        temp=addrToModify[i];
        addrToModify[i]=addrToModify[j];
        addrToModify[j]=temp;
    }
    } else if (dt == DT_BOOL) {
      sprintf(addrToModify,"%i",value->v.boolV);//Convert bool to string
    } else if (dt == DT_STRING) { /*In case it is a String, the size will be equal to the corresponding type length*/
      sprintf(addrToModify,"%s",value->v.stringV);
    }
    return RC_OK;
  }
  /*If attrNum is greater than numAttr - Set the error message*/
  RC_message = "attrNum is greater than the available number of Attributes";
  return RC_RM_NO_MORE_TUPLES;
}

//the method deals with records, it is used 
//to insert a record in a page
RC insertRecord (RM_TableData *rel, Record *record){
  int slotNum=0;
  char *space=NULL;
  RID id;
  int pLength,totalrecordlength;  
  //creating a page  
  PageNumber pageNum;
  //creating pointers to manage the data
  BM_BufferPool *buffer=(BM_BufferPool *)rel->mgmtData;
  BM_PageHandle *pHandle=MAKE_PAGE_HANDLE();
  SM_FileHandle *sh=(SM_FileHandle *)buffer->mgmtData;
  pageNum=1;
  //calculating the size of the record to calculate the 
  //page number of an empty slot
  totalrecordlength=getRecordSize(rel->schema);
  while(pageNum < sh->totalNumPages){
      pinPage(buffer,pHandle,pageNum);
      pLength=strlen(pHandle->data);
      //empty slot found
      if(PAGE_SIZE-pLength > totalrecordlength)
      {
          slotNum=pLength/totalrecordlength;
          unpinPage(buffer,pHandle);
          break;
      }
      unpinPage(buffer,pHandle);
      pageNum++;
  //appending the file because no empty slots
  }if(slotNum==0){
    pinPage(buffer,pHandle,pageNum + 1);
    unpinPage(buffer,pHandle);
  }
  pinPage(buffer,pHandle,pageNum);
  space=pHandle->data+strlen(pHandle->data);
  //copying record->data to the freespace
  strcpy(space,record->data);
  //marking as dirty
  markDirty(buffer,pHandle);
  unpinPage(buffer,pHandle);
  
  id.page=pageNum;
  id.slot=slotNum;
  //setting slot info
  record->id=id;

  return RC_OK;
}

//the method deals with records, it is used to 
//remove depending on RID
RC deleteRecord (RM_TableData *rel, RID id){
  int slotNum=id.slot;
  int recLength;
  //creating pointers to manage the data  
  BM_BufferPool *buffer=(BM_BufferPool *)rel->mgmtData;
  BM_PageHandle *pHandle=MAKE_PAGE_HANDLE();
  PageNumber pageNum=id.page; 
  //size to be removed   
  recLength=getRecordSize(rel->schema);
  pinPage(buffer,pHandle,pageNum);

  markDirty(buffer,pHandle);
  unpinPage(buffer,pHandle);
  free(pHandle);
  return RC_OK;
}

//the method deals with records, it is used to update 
//the record with new values
RC updateRecord (RM_TableData *rel, Record *record){
  char *space;
  int recLength;
  //creating pointers to manage the data
  BM_BufferPool *buffer=(BM_BufferPool *)rel->mgmtData;
  BM_PageHandle *pHandle=MAKE_PAGE_HANDLE();
  RID id=record->id;
  //slot to be updated
  int slotNum=id.slot;
  //page to be updated
  PageNumber pageNum=id.page;
  //space of slot to be updated
  recLength=getRecordSize(rel->schema);
  pinPage(buffer,pHandle,pageNum);
  //address of updating content
  space=pHandle->data+recLength*slotNum;
  //copying the content
  strncpy(space,record->data,recLength);

  return RC_OK;
}

//the method deals with records, it is used to get a record value
//depending on the RID and assign the value to the record
RC getRecord (RM_TableData *rel, RID id, Record *record){
  int slotNum=id.slot;
  char *space;
  int recLength;
  //creating pointers to manage the data
  BM_BufferPool *buffer=(BM_BufferPool *)rel->mgmtData;
  BM_PageHandle *pHandle=MAKE_PAGE_HANDLE();
  //getting the id of the page
  PageNumber pageNum=id.page;
  //getting the size of the record of the schema
  recLength=getRecordSize(rel->schema);
  pinPage(buffer,pHandle,pageNum);
  space=pHandle->data+recLength*slotNum;
  //copying the content
  strncpy(record->data,space,recLength);
  markDirty(buffer,pHandle);
  unpinPage(buffer,pHandle);

  return RC_OK;
}

//This method starts a new scan
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
  //creating pointers to manage the data
  BM_BufferPool *buffer=(BM_BufferPool *)rel->mgmtData;
  SM_FileHandle *fHandle=(SM_FileHandle *)buffer->mgmtData;
  //allocating the auxiliary scan in memory
  AUX_Scan *sHelp=(AUX_Scan *)malloc(sizeof(AUX_Scan));
  //Seting values 
  scan->mgmtData= cond;
  scan->rel=rel;
  //setting values of the auxiliary scan
  sHelp->_slotID=1;
  sHelp->_sPage=1;
  sHelp->_numPages=fHandle->totalNumPages;
  sHelp->pHandle=MAKE_PAGE_HANDLE();
  sHelp->_recLength=getRecordSize(rel->schema);
  //assigning values of the auxiliary scan to the scan
  return insert(&RM_Scan_stptr,scan,sHelp);
  
}
//This method scans the records until it finds
//a tuple that matches.
RC next (RM_ScanHandle *scan, Record *record){
  RID id;
      int pagelength;

  Expr *expression=(Expr *)scan->mgmtData,*l,*r,*auxExpr;
  RM_TableData *re=scan->rel;
  Operator *decision,*decision2;
  AUX_Scan *AUX_Scan=search(RM_Scan_stptr,scan);
  //Column value that has to be revised in the tuple
  Value **cValue=(Value **)malloc(sizeof(Value *));
  //Flag used to know if there is a match
  bool match=FALSE;
  *cValue=NULL;
  decision=expression->expr.op;
  //depending on the operation of the expression...
  if(expression==NULL)
    {

        //1.Return all the tuples.
        //2.Once all the tuples returned... return no more tuples.
      if(AUX_Scan->_sPage < AUX_Scan->_numPages)//Scan through all the pages of the table.
      {
          pinPage(re->mgmtData,AUX_Scan->pHandle,AUX_Scan->_sPage);
          pagelength=strlen(AUX_Scan->pHandle->data);
          AUX_Scan->_recsPage=pagelength/AUX_Scan->_recLength;
          if(AUX_Scan->_slotID < AUX_Scan->_recsPage)//scan through all the slots and get the record.
          {
              id.page=AUX_Scan->_sPage;
              id.slot=AUX_Scan->_slotID;
              getRecord(re,id,record);
              AUX_Scan->_slotID++;
          }
          else
          {
              AUX_Scan->_sPage+=1;
              AUX_Scan->_slotID=1;
          }
          unpinPage(re->mgmtData,AUX_Scan->pHandle);
          free(cValue[0]);
          free(cValue);
          return RC_OK;
      }
      else
      {
          free(cValue[0]);
          free(cValue);
          return RC_RM_NO_MORE_TUPLES;//Once done with the scans.. returns no more tuples to indicate end of the scan.
      }
    }else{
  switch(decision->type){
    case OP_COMP_SMALLER:
      l=decision->args[0];
      r=decision->args[1];
      //checking all pages
      while(AUX_Scan->_sPage < AUX_Scan->_numPages){
          pinPage(re->mgmtData,AUX_Scan->pHandle,AUX_Scan->_sPage);
          pagelength=strlen(AUX_Scan->pHandle->data);

          AUX_Scan->_recsPage=strlen(AUX_Scan->pHandle->data)/AUX_Scan->_recLength;
          //checking all records in the page
          while(AUX_Scan->_slotID < AUX_Scan->_recsPage){
              id.slot=AUX_Scan->_slotID;
              id.page=AUX_Scan->_sPage;
              //getting the record of this id
              getRecord(re,id,record);
              //getting the aattribute of the record to see if it matches
              getAttr(record,re->schema,r->expr.attrRef,cValue);
              //match found
              if((re->schema->dataTypes[r->expr.attrRef]==DT_INT)&&(l->expr.cons->v.intV>cValue[0]->v.intV)){
                 AUX_Scan->_slotID++;
                 unpinPage(re->mgmtData,AUX_Scan->pHandle);
                 match=TRUE;
                 break;
              //checking next slot
              }AUX_Scan->_slotID++;
          } break;
      }break;
    case OP_COMP_EQUAL:
      l=decision->args[0];
      r=decision->args[1];
      //checking all pages
      while(AUX_Scan->_sPage < AUX_Scan->_numPages){
          pinPage(re->mgmtData,AUX_Scan->pHandle,AUX_Scan->_sPage);
          pagelength=strlen(AUX_Scan->pHandle->data);
          AUX_Scan->_recsPage=strlen(AUX_Scan->pHandle->data)/AUX_Scan->_recLength;
          //checking all records in the page
          while(AUX_Scan->_slotID < AUX_Scan->_recsPage){
              id.page=AUX_Scan->_sPage;
              id.slot=AUX_Scan->_slotID;
              //getting the record of this id
              getRecord(re,id,record);
              //getting the aattribute of the record to see if it matches
              getAttr(record,re->schema,r->expr.attrRef,cValue);
              //match found
              if((re->schema->dataTypes[r->expr.attrRef]==DT_STRING)&&(strcmp(cValue[0]->v.stringV , l->expr.cons->v.stringV)==0)){
                 AUX_Scan->_slotID++;
                 unpinPage(re->mgmtData,AUX_Scan->pHandle);
                 match=TRUE;
                 break;
              //match found
              }else if((re->schema->dataTypes[r->expr.attrRef]==DT_INT)&&(cValue[0]->v.intV == l->expr.cons->v.intV)){
                  AUX_Scan->_slotID++;
                  unpinPage(re->mgmtData,AUX_Scan->pHandle);
                  match=TRUE;
                  break;
              //match found
              }else if((cValue[0]->v.floatV == l->expr.cons->v.floatV)&&(re->schema->dataTypes[r->expr.attrRef]==DT_FLOAT)){
                  AUX_Scan->_slotID++;
                  unpinPage(re->mgmtData,AUX_Scan->pHandle);
                  match=TRUE;
                  break;
              //checking next slot
              } AUX_Scan->_slotID++;
          }break;     
      }break; 
    case OP_BOOL_NOT:
        //using an auxiliary expresion to find a match
        auxExpr=expression->expr.op->args[0];
        decision2=auxExpr->expr.op;
        r=decision2->args[0];
        l=decision2->args[1];
        if (decision2->type==OP_COMP_SMALLER){
          //checking all pages
          while(AUX_Scan->_numPages>AUX_Scan->_sPage){
              pinPage(re->mgmtData,AUX_Scan->pHandle,AUX_Scan->_sPage);
              pagelength=strlen(AUX_Scan->pHandle->data);
              AUX_Scan->_recsPage=strlen(AUX_Scan->pHandle->data)/AUX_Scan->_recLength;
              //checking all records in the page
              while(AUX_Scan->_slotID < AUX_Scan->_recsPage){
                  id.slot=AUX_Scan->_slotID;
                  id.page=AUX_Scan->_sPage;
                  //getting the record of this id
                  getRecord(re,id,record);
                  //getting the aattribute to see if it matches
                  getAttr(record,re->schema,r->expr.attrRef,cValue);
                  if((cValue[0]->v.intV > l->expr.cons->v.intV)&&(re->schema->dataTypes[r->expr.attrRef]==DT_INT)){
                      AUX_Scan->_slotID++;
                      unpinPage(re->mgmtData,AUX_Scan->pHandle);
                      match=TRUE;
                      break;
                  //checking next slot
                  }AUX_Scan->_slotID++;
              }break; 
          }break;
        }break;
  }
  free(*cValue);//free the column value we created.
      free(cValue);
      if(match==TRUE) return RC_OK;
  else return RC_RM_NO_MORE_TUPLES;
}}

//this method closes the started scan
RC closeScan (RM_ScanHandle *scan){
  AUX_Scan *auxScan=search(RM_Scan_stptr,scan);
  return delete(&RM_Scan_stptr,scan);
}

//This two methods should one initialize and the other shutdown 
//a record manager, but since we do not use any global structure
//it is not required to be implemented.
RC initRecordManager (void *mgmtData){return RC_OK;}
RC shutdownRecordManager (){return RC_OK;}

//This method creates a new table storing the schema 
//in the first pagethrough the previous assignment 
//buffer manager.
RC createTable (char *name, Schema *schema){
    char filename[100]={'\0'};
    int i,pos=0;
    strcat(filename,name);
    strcat(filename,".bin");//We create the page file as  a binary file with "bin" extension.
    createPageFile(filename);
    BM_PageHandle *ph=MAKE_PAGE_HANDLE();
    BM_BufferPool *bm=MAKE_POOL();
    initBufferPool(bm,filename,1,RS_FIFO,NULL);
    pinPage(bm,ph,0);
    for(i=0;i < schema->numAttr;i++)
    {
      pos+=sprintf(ph->data+pos,"Numattr-%d,DataType[%d]-%d,Typelength[%d]=%d",schema->numAttr,i,schema->dataTypes[i],i,schema->typeLength[i]);//Write the schema by getting the offset.
    }
    markDirty(bm,ph);
    unpinPage(bm,ph);
    forceFlushPool(bm);
    shutdownBufferPool(bm);
    gSchema=*schema;  return RC_OK;
}

//The method opens a table we have created through
//the previous assignment buffer manager.
RC openTable (RM_TableData *rel, char *name){
  BM_BufferPool *buffer=MAKE_POOL();
  //allocating memory for the schema
  Schema *schema=(Schema *)malloc(sizeof(Schema));
  char fname[100]={'\0'};
  strcat(fname,name);
  strcat(fname,".bin");
  //initializing pool
  initBufferPool(buffer,fname,4,RS_FIFO,NULL);
  rel->name=name;
  rel->mgmtData=buffer;  
  *schema=gSchema;
  //setting values of the opened page
  rel->schema=schema;
  return RC_OK;
}

//This method closes the table we have created 
//by shutting down the pool
RC closeTable (RM_TableData *rel){
  BM_BufferPool *bm=(BM_BufferPool *)rel->mgmtData;
    freeSchema(rel->schema);//Free up the schema.
    shutdownBufferPool(bm);//Shutdown the pool associated with that table.
    free(bm);
    return RC_OK;
}

//This method deletes the table we have created
RC deleteTable (char *name){
  char fname[100]={'\0'};
  strcat(fname,name);
  strcat(fname,".bin");
  destroyPageFile(fname);
  return RC_OK;
}

//This method returns the total number 
//of touples in a page.
int getNumTuples (RM_TableData *rel){
  int tuplesCount=0,pLength;
  int i;
  PageNumber pageNum=1;
  //creating pointers to manage the data  
  BM_PageHandle *pagehandle=MAKE_PAGE_HANDLE();
  BM_BufferPool *buffer=(BM_BufferPool *)rel->mgmtData;
  SM_FileHandle *fHandle=(SM_FileHandle *)buffer->mgmtData;
  PageNumber numPages=fHandle->totalNumPages;
  //checking all the pages
  while(pageNum < numPages){
    pinPage(buffer,pagehandle,pageNum);
    pageNum++;
    for(i=0;i < PAGE_SIZE;i++){
        //if a new touple is found, the count is updated
        if(tuplesCount=pagehandle->data[i]=='-') tuplesCount++;
    }
  }
  return tuplesCount;
}
