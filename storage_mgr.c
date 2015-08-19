#include "dberror.h"
#include "storage_mgr.h"

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

void initStorageManager (void){}


/* Manipulating files */

// creates a page just to read and write with the size = PAGE_SIZE
RC createPageFile (char *fileName){

    // file exists?
    if(access(fileName, F_OK)==0){
        return RC_OK; 
    }
    // if not, create page to read, write
    FILE *file=fopen(fileName,"wb");
    // filling the page with zeros to get the PAGE_SIZE size
    int i;
    for(i=0; i<PAGE_SIZE; i++){
        fprintf(file,"%c",'\0');
    }

    // file closing
    fclose(file);

    return RC_OK;
}


// opens a page to read and write
RC openPageFile (char *fileName, SM_FileHandle *fHandle){

    // file exists?
    if(access(fileName, F_OK)==0){
        // it exists so it is opened
        FILE *file = fopen(fileName,"rb+");

        // it calculates the pages of the file
        struct stat st;
        stat(fileName, &st);
        int totalPages = (st.st_size-1)/PAGE_SIZE;

        // fHandle initialization
        fHandle->totalNumPages = totalPages+1;
        fHandle->fileName=fileName;
        fHandle->mgmtInfo = file;
        fHandle->curPagePos=0;

        return RC_OK;
    }

    return RC_FILE_NOT_FOUND;
}

// closes a page getting memory free
RC closePageFile (SM_FileHandle *fHandle){
    // fHandle initialized?
    if(fHandle==NULL){
        return RC_FILE_HANDLE_NOT_INIT;
    }
    // assuring it is closed correctly
    if(fclose(fHandle->mgmtInfo)==0){
        return RC_OK;
    }
    else{
        // it has not been closed correctly
        return RC_FILE_NOT_FOUND;
    }
}

// removes the file
RC destroyPageFile (char *fileName){
    // assuring file has been deleted correctly;
    if(remove(fileName)!=0){
        return RC_FILE_NOT_FOUND;
    }
    return RC_OK;
}


// read a block from file to memory
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    // setting the fHandle pointer to the file 
    FILE *file=fHandle->mgmtInfo;
    char *fileName = fHandle->fileName;
    int nump = fHandle->totalNumPages;    
    
    // assuring: file exists
    if(file==NULL||pageNum > nump ){
        return RC_READ_NON_EXISTING_PAGE;
    }
    // setting the pointer to the page number pageNum
    fseek(file,pageNum * PAGE_SIZE, SEEK_SET);
    // assuring the read has been successful
    fread(memPage, PAGE_SIZE,1,file); 

    // updating fHandle curPAgePos
    fHandle->curPagePos= pageNum;
    return RC_OK;
}

// gets the current block position
int getBlockPos (SM_FileHandle *fHandle){   
    // returns an integer with the curPagePos ( current position )
    return fHandle->curPagePos;;
}

// reads first block of the file
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    // setting the fHandle pointer to the file 
    FILE *file=fHandle->mgmtInfo;
    char *fileName = fHandle->fileName;
    // assuring: file exists
    if(file==NULL ){
        printError(RC_READ_NON_EXISTING_PAGE);
        return RC_READ_NON_EXISTING_PAGE;
    }
    // setting the pointer to the page number pageNum
    fseek(file,0, SEEK_SET);
    rewind(file);
    // assuring the read has been successful
    fread(memPage, PAGE_SIZE,1,file); 

    // updating fHandle curPAgePos
    fHandle->curPagePos= 1;
    return RC_OK;
}

//Read last page of the file.
RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    int pageNum = fHandle->totalNumPages;
    // setting the fHandle pointer to the file 
    FILE *file=fHandle->mgmtInfo;
    char *fileName = fHandle->fileName;
    int nump = fHandle->totalNumPages;    

    // assuring: file exists
    if(file==NULL ){
        return RC_READ_NON_EXISTING_PAGE;
    }
    // setting the pointer to the page number pageNum
    fseek(file,PAGE_SIZE*(pageNum-1), SEEK_SET);
    // assuring the read has been successful
    fread(memPage, PAGE_SIZE,1,file); 

    // updating fHandle curPAgePos
    fHandle->curPagePos= nump;
    return RC_OK;
}

// reads the current block
RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    // sets the pointer to the current block
    int pageNum = fHandle->curPagePos;
    // setting the fHandle pointer to the file 
    FILE *file=fHandle->mgmtInfo;
    char *fileName = fHandle->fileName;
    int pos = fHandle->curPagePos;
    int nump = fHandle->totalNumPages;    
    // assuring: file exists
    if(file==NULL|| pos <0 || pos > nump ){
        return RC_READ_NON_EXISTING_PAGE;
    }
    // setting the pointer to the page number pageNum
    fseek(file,PAGE_SIZE*(pageNum-1), SEEK_CUR);
    // assuring the read has been successful
    fread(memPage, PAGE_SIZE,1,file); 

    return RC_OK;
}

// reads the previous block
RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    // sets the pointer to the previous block
    int pageNum = fHandle->curPagePos;
    // setting the fHandle pointer to the file 
    FILE *file=fHandle->mgmtInfo;
    char *fileName = fHandle->fileName;
    int pos = fHandle->curPagePos;
    // assuring: file exists
    if(file==NULL|| pos-2 <0 ){
        return RC_READ_NON_EXISTING_PAGE;
    }
    // setting the pointer to the page number pageNum
    fseek(file,PAGE_SIZE*(pageNum-2), SEEK_SET);
    // assuring the read has been successful
    fread(memPage, PAGE_SIZE,1,file); 

    fHandle->curPagePos = pageNum-1;
    return RC_OK;
}

// reads the next block
RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    // sets the pointer to the next block
    int pageNum = fHandle->curPagePos;
    // setting the fHandle pointer to the file 
    FILE *file=fHandle->mgmtInfo;
    char *fileName = fHandle->fileName;
    int pos = fHandle->curPagePos;
    int nump = fHandle->totalNumPages;

    // assuring: file exists
    if(file==NULL|| pos+1 > nump ){
        return RC_READ_NON_EXISTING_PAGE;
    }
    // setting the pointer to the page number pageNum
    fseek(file,1, SEEK_CUR);
    // assuring the read has been successful
    fread(memPage, PAGE_SIZE,1,file); 

    fHandle->curPagePos = pageNum+1;
    return RC_OK;
}


/* writing blocks to a page file */

// auxiliar method called every time it is needed to write into a page
static RC writePage(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // setting the fHandle pointer to the file 
    FILE *file=fHandle->mgmtInfo;
    char *fileName = fHandle->fileName;
    // assuring: file exists
    if(file == NULL){
        // new error defined in dberror.h
        return RC_WRITE_FAILED;
    }
    // assuring we do not try to access out of the page
    if(pageNum < -1){
    //     // new error defined in dberror.h
         return RC_WRITE_FAILED; 
     }

    if (pageNum > fHandle->totalNumPages){
        fHandle->totalNumPages = pageNum +1;
    }
    // seting the pointer to the page number pageNum
    fseek(file, pageNum * PAGE_SIZE, 0);
    // assuring the write has been successful
    fwrite(memPage,PAGE_SIZE,1,file); 
    if (ferror(file)||feof(file)) {
        return RC_WRITE_FAILED;
    }

    // updating fHandle curPAgePos
    //fHandle->curPagePos= pageNum+1;
    return RC_OK;
}

// writes a block in the file
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    // calls writePage with the specified parameters, and returns RC
    return writePage(pageNum, fHandle, memPage);
}


// appends one extra page to the file. filled with 0
 RC appendEmptyBlock (SM_FileHandle *fHandle){
    FILE *file=fHandle->mgmtInfo;
    // writes a page size with 0
    if(file==NULL ){
        return RC_FILE_NOT_FOUND;
    }
    // writes a page size filled with 0 in file
    int i;
    for(i=0; i<PAGE_SIZE; i++){
        fprintf(file,"%c",'\0');
    }  
    // update curPagePos
    fHandle->totalNumPages+=1;

    return RC_OK;
    
}

// writes in the current block of the file
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    // sets the pointer to the current block
    int pageNum = (fHandle->curPagePos);
    // calls writePage with the specified parameters, and returns RC
    return writePage(pageNum, fHandle, memPage);
}

// ensure capacity: enlarges number of pages to numberOfPages if needed
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle){
    // checks if there are already enough pages
    if (fHandle->totalNumPages < numberOfPages){
        // if not, calculate how many pages we need
        int addPages = (numberOfPages-fHandle->totalNumPages)*PAGE_SIZE;
        // adds this number of pages
        int i;
        for (i=0; i<addPages; i++){
            fprintf(fHandle->mgmtInfo, "%c",'\0');
        }
        fHandle->totalNumPages=numberOfPages;
        return RC_OK;
    } else {
        // new error defined in dberror.h, already have enough number of pages
        return RC_ENOUGH_NUMB_PAGES;
    }
}
