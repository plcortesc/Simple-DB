#include "buffer_list.h"
#include <stdio.h>
#include <stdlib.h>


// insert new buffer pool in the list
RC insert_bufpool(EntryPointer *entry, void *buffer_pool_ptr, void *buffer_page_handle){
    
    EntryPointer newptr;
    EntryPointer previousptr;
    EntryPointer currentptr;
    newptr =(EntryPointer)malloc(sizeof(BufferPool_Entry));
    if(newptr!=NULL){
        newptr->buffer_pool_ptr=buffer_pool_ptr;
        newptr->buffer_page_info=buffer_page_handle;
        newptr->numreadIO=0;
        newptr->numwriteIO=0;
        newptr->nextBufferEntry=NULL;

        previousptr=NULL;
        currentptr =*entry;

        while (currentptr!=NULL ){
            previousptr=currentptr;
            currentptr=currentptr->nextBufferEntry;
        }
        if(previousptr==NULL){
            *entry=newptr;
        }else{
            previousptr->nextBufferEntry=newptr;
        }
        return RC_OK;
    }
    //new error defined
    return RC_INSERT_BUFPOOL_FAILED;
}

// find the buffer pool in the list, for the entryptr given
BufferPool_Entry *find_bufferPool(EntryPointer entryptr, void *buffer_pool_ptr){
    
    EntryPointer previousptr=NULL;
    EntryPointer currentptr=entryptr;
    while(currentptr!=NULL){
        if(currentptr->buffer_pool_ptr==buffer_pool_ptr){
            break;
        }
        previousptr=currentptr;
        currentptr=currentptr->nextBufferEntry;
    }

    return currentptr;
}


// Delete the entry of the buffer pool entryptr given as a parameter
bool delete_bufpool(EntryPointer *entryptr, void *buffer_pool_ptr ){
    EntryPointer temptr;
    EntryPointer previousptr;
    EntryPointer currentptr;
    previousptr=NULL;
    currentptr=*entryptr;

    while(currentptr!=NULL && currentptr->buffer_pool_ptr!=buffer_pool_ptr){
        previousptr=currentptr;
        currentptr=currentptr->nextBufferEntry;
    }
    
    temptr=currentptr;

    if(previousptr== NULL){
        *entryptr=currentptr->nextBufferEntry;
    }else{
        previousptr->nextBufferEntry=currentptr->nextBufferEntry;
    }
    free(temptr);
    
    return TRUE;

}

// check if there are buffer pools using the same file
BufferPool_Entry *checkPoolsUsingFile(EntryPointer entry, int *filename){
    
    EntryPointer previousptr=NULL;
    EntryPointer currentptr=entry;
    BM_BufferPool *bufferpool;
    while(currentptr!=NULL){
        bufferpool=(BM_BufferPool *)currentptr->buffer_pool_ptr;
        if(bufferpool->pageFile==filename){
           break;
        }
        previousptr=currentptr;
        currentptr=currentptr->nextBufferEntry;
    }

    return currentptr;
}

// return the number of buffer pool using the given file
int getPoolsUsingFile(EntryPointer entry, char *filename){
    
    EntryPointer previousptr=NULL;
    EntryPointer currentptr=entry;
    BM_BufferPool *bufferpool;
    //return
    int count=0;
    // for every entry, check if it is using the file
    while(currentptr!=NULL){
        bufferpool=(BM_BufferPool *)currentptr->buffer_pool_ptr;
        if(bufferpool->pageFile==filename){
            count++;
        }
        previousptr=currentptr;
        currentptr=currentptr->nextBufferEntry;
    }

    return count;
}
