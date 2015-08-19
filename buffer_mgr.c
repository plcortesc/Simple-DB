#include "buffer_mgr.h"
#include "buffer_list.h"
#include "storage_mgr.h"
#include "dt.h"
#include <stdio.h>
#include <stdlib.h>

// static
// pointer to the buffer pool entry in the list
static EntryPointer BP_Entry_pointer=NULL;
// used for timestamps
static long double univtime=-32674;
// static for initFrames auxiliar method
static char *initFrames(const int numPages);
// static for findReplace auxiliar method
static Buffer_page_info *findReplace(BM_BufferPool *const bm, BufferPool_Entry *bp_entry);


RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData){

    int i;
    // flag
    bool insertOK;
    Buffer_page_info *bf_pg_info;
    BufferPool_Entry *_bufferentry;
    BM_BufferPool *tempPool;
    SM_FileHandle *fileHandle;
    // check if there are buf pools using the same file
    _bufferentry=checkPoolsUsingFile(BP_Entry_pointer, pageFileName);
    //if it is null, it is not being used by any buffer pool.

    // first we check if buffer pool does not exit, so we create a new one and insert it
    if(_bufferentry==NULL){
        fileHandle=(SM_FileHandle *)malloc(sizeof(SM_FileHandle));

        // init
        bm->numPages=numPages;
        bm->strategy=strategy;
        bm->pageFile=pageFileName;
        bm->mgmtData=fileHandle;
        openPageFile(pageFileName,fileHandle);

        // init buffer details as specified
        bf_pg_info = (Buffer_page_info *)calloc(numPages,sizeof(Buffer_page_info));
        for(i=0;i < numPages;i++){
            (bf_pg_info+i)->pageframes=initFrames(numPages);
            bf_pg_info[i].fixcounts=0;
            bf_pg_info[i].isdirty=FALSE;
            bf_pg_info[i].pagenums=NO_PAGE;
        }

        //insert the new buffer pool to the list
        return insert_bufpool(&BP_Entry_pointer,bm,bf_pg_info);
        
    }
    // if it already exists (_bufferentry!=NULL), we have two buffer pools sharing resources
    else{
        // update
        tempPool=_bufferentry->buffer_pool_ptr;
        bm->pageFile=pageFileName;
        bm->numPages=numPages;
        bm->strategy=strategy;
        bm->mgmtData=tempPool->mgmtData;
        bf_pg_info=_bufferentry->buffer_page_info;
        //insert the new buffer pool to the list
        return insert_bufpool(&BP_Entry_pointer, bm, bf_pg_info);
    }

}

// auxiliar method to allocate memory of the page frames and init page frames for the buffer pool
static char *initFrames(const int numPages){
    int i;
    char *frames;
    frames=(char *)malloc(PAGE_SIZE);
    // init with zeros
    if(frames!=NULL){
        for(i=0;i < PAGE_SIZE ; i++)
            frames[i]='\0';
    }
    return frames;
}


RC shutdownBufferPool(BM_BufferPool *const bm){

    int i;
    int *pgnums, *fixCounts;
    // flags
    bool ispinned=FALSE;
    int numberPools;
    // ptr
    Buffer_page_info *pg_info;
    BufferPool_Entry *bufentry;
    char *frame;
    // getters
    fixCounts=getFixCounts(bm);
    pgnums=getFrameContents(bm);

    // check if there is any page that is currently pinned. before shut down the buffer pool
    for(i=0 ; i < bm->numPages ; i++){
        if(fixCounts[i] > 0){
            ispinned=TRUE;
        }
    }
    free(fixCounts);
    //if no pinned pages
    if(ispinned==FALSE){
        // find the current buffer pool from the list
        bufentry=find_bufferPool(BP_Entry_pointer,bm);
        // check if it shares resources with some other buffer pool, and return how many of them
        numberPools=getPoolsUsingFile(BP_Entry_pointer,bm->pageFile);
        // if numberPools=1, it means it doesnt share resources, so we can free them
        // it is that it has been only accessed this time

        // get info
        pg_info=bufentry->buffer_page_info;
        // for every frame of the buffer pool
        for(i=0;i<bm->numPages;i++){
            frame=pg_info[i].pageframes;
            // if page is dirty, write it to disk
            if(pg_info[i].isdirty==TRUE){
               if (writeBlock(pg_info[i].pagenums,bm->mgmtData,frame)!=RC_OK){
                    return RC_WRITE_FAILED;
                }
            }
            // free respective frame
            if(numberPools == 1){
                free(frame);
            }
        }
        // free the buffer pool details
        if(numberPools == 1){
            free(pg_info);
        }
        pg_info=NULL;
        // delete buffer pool from the list
        delete_bufpool(&BP_Entry_pointer,bm);
        // free mgmtData
        if(numberPools == 1){
            closePageFile(bm->mgmtData);
            free(bm->mgmtData);
        }
    }
    return RC_OK;
}


RC forceFlushPool(BM_BufferPool *const bm){
    
    int i;
    Buffer_page_info *pg_info;
    BufferPool_Entry *bufentry;
    char *frame;
    // find the current buffer pool from the list
    bufentry=find_bufferPool(BP_Entry_pointer,bm);
    pg_info=bufentry->buffer_page_info;
    // for every page frame
    for(i=0;i<bm->numPages;i++){
        frame=pg_info[i].pageframes;
        // Need to check 2 conditions
        // page is dirty and fixcounts=0->no one has pinned this page
        if(pg_info[i].isdirty==TRUE && pg_info[i].fixcounts==0){
            // write page to disk
            if(writeBlock(pg_info[i].pagenums,bm->mgmtData,frame)!=RC_OK){
                return RC_WRITE_FAILED;
            }                   
            pg_info[i].isdirty=FALSE;
            bufentry->numwriteIO++;
        }
    }
    return RC_OK;
}


PageNumber *getFrameContents (BM_BufferPool *const bm){

    PageNumber i;
    PageNumber *pgnum=NULL;
    Buffer_page_info *bf_pg_info;
    // find the current buffer pool from the list
    EntryPointer _bufferNode=find_bufferPool(BP_Entry_pointer,bm);
    if(_bufferNode!=NULL){ 
        // create arry to save the frame contents
        pgnum=(PageNumber *)calloc(bm->numPages,sizeof(PageNumber));
        // get the pages of the buffer pool
        bf_pg_info=_bufferNode->buffer_page_info;
        //for each page of the buffer in each position, save it in pgnum array
        for(i=0;i < bm->numPages; i++){
            pgnum[i]=bf_pg_info[i].pagenums;
        }
    }
    return pgnum;
}


int *getFixCounts (BM_BufferPool *const bm){
    
    int i;
    int *fixcounts;
    Buffer_page_info *bf_pg_info;
    // find the current buffer pool from the list
    EntryPointer _bufferentry=find_bufferPool(BP_Entry_pointer,bm);
    // create array to save the fixcounts
    fixcounts=(int *)calloc(bm->numPages,sizeof(int));
    bf_pg_info=_bufferentry->buffer_page_info;
    if(bf_pg_info!=NULL){
        // for every page, get its fixcounts and save it into the array
        for(i=0 ; i < bm->numPages ; i++){
          fixcounts[i]=bf_pg_info[i].fixcounts;
        }
    }
    else{
      free(bf_pg_info);
      bf_pg_info=NULL;
    }

    return fixcounts;
}


bool *getDirtyFlags (BM_BufferPool *const bm){
    
    int i;
    bool  *dirtyflags;
    Buffer_page_info *bf_pg_info;
    // find the current buffer pool from the list
    EntryPointer _bufferNode=find_bufferPool(BP_Entry_pointer,bm);
    // create array to save dirty pages
    dirtyflags=(bool *)calloc(bm->numPages,sizeof(bool));
    bf_pg_info=_bufferNode->buffer_page_info;
    if(bf_pg_info!=NULL){
        //for every page, check if it is dirty and add it to array
        for(i=0;i < bm->numPages;i++){
            dirtyflags[i]=bf_pg_info[i].isdirty;
        }
    }else{
        free(bf_pg_info);
        bf_pg_info=NULL;
    }

    return dirtyflags;
}


int getNumReadIO (BM_BufferPool *const bm){

    // find the current buffer pool from the list
    EntryPointer _bufferNode=find_bufferPool(BP_Entry_pointer,bm);
    // and get the value

    return _bufferNode->numreadIO;
}


int getNumWriteIO (BM_BufferPool *const bm){
    
    // find the current buffer pool from the list
    EntryPointer _bufferentry=find_bufferPool(BP_Entry_pointer,bm);
    // and get the value

    return _bufferentry->numwriteIO;
}



RC markDirty(BM_BufferPool * const bm, BM_PageHandle * const page) {
	
    int i;
	BufferPool_Entry *pg_entry;
	Buffer_page_info *bf_pg_info;
    // find the current buffer pool from the list
	pg_entry = find_bufferPool(BP_Entry_pointer, bm);
	bf_pg_info = pg_entry->buffer_page_info;
    // look for the page in the buffer pool that is in the same position as the one given 'page'
	for (i = 0; i < bm->numPages; i++) {
        // when we find the correspondent page, mark it as dirty
		if (((bf_pg_info + i)->pagenums) == page->pageNum) {
			(bf_pg_info + i)->isdirty = TRUE;
			return RC_OK;
		}
	}
    // new error defined
    return RC_MARK_DIRTY_FAILED;
}


RC forcePage(BM_BufferPool * const bm, BM_PageHandle * const page) {
    
    BufferPool_Entry *bp_entry;
    // find the current buffer pool from the list
    bp_entry = find_bufferPool(BP_Entry_pointer, bm);
    // write the content of the page given to disk
    if (bm->mgmtData!= NULL) {
        writeBlock(page->pageNum, bm->mgmtData, page->data);
        // increment number of writeIO
        bp_entry->numwriteIO++;
        return RC_OK;
    }

    // new error defined
    return RC_FORCE_PAGE_ERROR;
}


RC unpinPage(BM_BufferPool * const bm, BM_PageHandle * const page) {

	int i;
    int totalPages = bm->numPages;
	BufferPool_Entry *bp_entry;
    Buffer_page_info *pg_info;
    // find the current buffer pool from the list
	bp_entry = find_bufferPool(BP_Entry_pointer, bm);
	pg_info = bp_entry->buffer_page_info;
    // find the given page between all pages in buffer pool
	for (i = 0; i < totalPages; i++) {
		if ((pg_info + i)->pagenums == page->pageNum){
            // decrement fixcount of respective page for being unpinned
			(pg_info + i)->fixcounts -= 1;
			return RC_OK;
		}
	}
    // new error defined
	return RC_UNPIN_FAILED;
}



RC pinPage(BM_BufferPool * const bm, BM_PageHandle * const page, const PageNumber pageNum) {
	
    int i;

    RC pinOK=RC_OK;
    
	Buffer_page_info *possibleReplace;
	BufferPool_Entry *bp_entry;
    // find the current buffer pool from the list
	bp_entry = find_bufferPool(BP_Entry_pointer, bm);
    // get its info
	Buffer_page_info *pg_info;
	pg_info = bp_entry->buffer_page_info;

	int emptyFlag = 1;
	if (pg_info != NULL) {

		// check if given page is already in the buffer pool's pages
		for (i=0; i<bm->numPages; i++){			
			emptyFlag = 0;
			/*To check if the page is already present in buffer.*/
			if ((pg_info + i)->pagenums == pageNum) {
                
                // for LRU only
                (pg_info + i)->timeStamp=univtime++;
                
                page->pageNum = pageNum;
				page->data = (pg_info + i)->pageframes;
                // increment fixcount of the given page as we are pinning it
				(pg_info + i)->fixcounts+=1;

				return RC_OK;
			}
		}

		// in case page is not already there, we look for the first empty page
		for (i = 0; i < bm->numPages; i++) {
            // possibleReplace will be the page whose pg_info is empty
            if ((pg_info + i)->pagenums == -1){
				possibleReplace = ((pg_info + i));
				emptyFlag = 1;
				break;
			}
		}
    }

	// if we find a free frame (emptyFlag=1), we can use this frame directly
	if (emptyFlag == 1) {
		
        page->pageNum = pageNum;
		page->data = possibleReplace->pageframes;
		
        pinOK=readBlock(pageNum, bm->mgmtData,possibleReplace->pageframes);
        if(pinOK==RC_READ_NON_EXISTING_PAGE||pinOK==RC_OUT_OF_BOUNDS){
            //If given page is not in the file, add it
            pinOK=appendEmptyBlock(bm->mgmtData);
	    }else{
            pinOK=RC_OK;
        }
        // increment numReadIO
        bp_entry->numreadIO++;
        // increment fixcounts
		possibleReplace->fixcounts += 1;
		possibleReplace->pagenums = pageNum;

        return pinOK;
	} 
    // if we are here, it is because there are not free frames -> need to REPLACE
    else {
		if (bm->strategy == RS_FIFO) {
            // call auxiliar method
			return applyFIFO(bm, page, pageNum);
		} else if (bm->strategy == RS_LRU) {
            // call auxiliar method
			return applyLRU(bm, page, pageNum);
        } else if (bm->strategy == RS_LRU) {
            // call auxiliar method
            return applyLFU(bm, page, pageNum);
        } else {
            // new defined error
			return RC_PIN_FAILED; 
		}
	}
	
}



RC applyFIFO(BM_BufferPool * const bm, BM_PageHandle * const page, const PageNumber pageNum) {
	
    BufferPool_Entry *bp_entry;
    // find the current buffer pool from the list
	bp_entry = find_bufferPool(BP_Entry_pointer, bm);
	Buffer_page_info *possibleReplace=NULL;
    // find the new possibleReplace with an auxiliar method
	possibleReplace = findReplace(bm, bp_entry);

    // flags
	RC writeOK = RC_OK;
	RC readOK = RC_OK;

    // if it is dirty, write it to disk
	if (possibleReplace->isdirty == TRUE) {
		writeOK = writeBlock(possibleReplace->pagenums, bm->mgmtData, possibleReplace->pageframes);
		// mark as not dirty
        possibleReplace->isdirty=FALSE;
        // increment numwriteIO
		bp_entry->numwriteIO++;
	}
    // read from disk to mem
	readOK = readBlock(pageNum, bm->mgmtData, possibleReplace->pageframes);
	if(readOK==RC_READ_NON_EXISTING_PAGE||readOK==RC_OUT_OF_BOUNDS||readOK==RC_READ_FAILED){
        // if given page is not there, add it
        readOK =appendEmptyBlock(bm->mgmtData);
    }

    //increment numreadIO and update info
    bp_entry->numreadIO++;
	page->pageNum  = pageNum;
	page->data = possibleReplace->pageframes;
    possibleReplace->pagenums = pageNum;
    possibleReplace->fixcounts+=1;
	possibleReplace->weight = possibleReplace->weight + 1;

    if (readOK == RC_OK && writeOK == RC_OK){
        return RC_OK;
	}else{
        //new defined error
		return RC_FIFO_FAILED;
    }
}



RC applyLRU(BM_BufferPool * const bm, BM_PageHandle * const page, const PageNumber pageNum) {
	
    BufferPool_Entry *bp_entry;
    // find the current buffer pool from the list
	bp_entry = find_bufferPool(BP_Entry_pointer, bm);
	Buffer_page_info *possibleReplace;
    // find the new possibleReplace with an auxiliar method
	possibleReplace = findReplace(bm, bp_entry);

    // flags
	RC writeOK = RC_OK;
	RC readOK = RC_OK;

    // if it is dirty, write to disk
	if (possibleReplace->isdirty == TRUE) {
		writeOK = writeBlock(page->pageNum, bm->mgmtData, possibleReplace->pageframes);
		// mark it as not dirty
        possibleReplace->isdirty=FALSE;
        //increment numwriteIO
		bp_entry->numwriteIO++;
	}
    // read from disk to mem
	readOK = readBlock(pageNum, bm->mgmtData, possibleReplace->pageframes);
	// increment numreadIO and update info
    bp_entry->numreadIO++;
    page->pageNum  = pageNum;
	page->data = possibleReplace->pageframes;
    possibleReplace->pagenums = pageNum;
    possibleReplace->fixcounts+=1;
	possibleReplace->weight+=1;
	possibleReplace->timeStamp =(long double)univtime++;

	if (readOK == RC_OK && writeOK == RC_OK){
        return RC_OK;
    }else{
        //new defined error
        return RC_LRU_FAILED;
    }
}


RC applyLFU(BM_BufferPool * const bm, BM_PageHandle * const page, const PageNumber pageNum) {

	BufferPool_Entry *bp_entry;
    // find the current buffer pool from the list
    bp_entry = find_bufferPool(BP_Entry_pointer, bm);
	Buffer_page_info *possibleReplace=NULL;
    // find the new possibleReplace with an auxiliar method
	possibleReplace = findReplace(bm, bp_entry);

    // flags
	RC writeOK = RC_OK;

    // if page is dirty, write to disk
	if (possibleReplace->isdirty == TRUE) {
		writeOK = writeBlock(possibleReplace->pagenums, bm->mgmtData, possibleReplace->pageframes);
		// mark as not dirty
        possibleReplace->isdirty=FALSE;
        // increment numwriteIOO
		bp_entry->numwriteIO++;
	}
    // read from disk to mem
	RC readOK = readBlock(pageNum, bm->mgmtData, possibleReplace->pageframes);
    if(readOK==RC_READ_NON_EXISTING_PAGE||readOK==RC_OUT_OF_BOUNDS||readOK==RC_READ_FAILED){
        // if given page is not there, add it
        readOK =appendEmptyBlock(bm->mgmtData);
    }
    //increment numreadIO and update info
    bp_entry->numreadIO++;
	page->pageNum  = pageNum;
	page->data = possibleReplace->pageframes;
    possibleReplace->pagenums = pageNum;
    possibleReplace->fixcounts+=1;
	possibleReplace->weight+=1;
	
    if (readOK == RC_OK && writeOK == RC_OK){
		return RC_OK;
	}else{
        // new error defined
		return RC_LFU_FAILED;
    }
}



Buffer_page_info *findReplace(BM_BufferPool * const bm, BufferPool_Entry *bp_entry) {
	
    int i;
    int count = 0;

    // first we need to find all pages that are not pinned, their fixcounts=0
	Buffer_page_info *bf_page_info = bp_entry->buffer_page_info;
    // define the variable where we are going to keep them
	Buffer_page_info *bf_page_info_zeros[bm->numPages];
    // init the array
	for (i=0; i< bm->numPages; i++){
		bf_page_info_zeros[i] = NULL;
	}
    // fill the array
	for (i=0; i< bm->numPages; i++){
        // if fixcount = 0, add to array
		if ((bf_page_info[i].fixcounts) == 0) {
			bf_page_info_zeros[count] = (bf_page_info+i);
			count++;
		}
	}

	// new macro defined, to calculate the size of the array with fixcount=0 pages
    #define sizeofa(array) sizeof array / sizeof array[0]

	// intermediate variable
    Buffer_page_info *next_bf_page_info;
    // final result -> replace
	Buffer_page_info *replace_bf_page_info;

    // assign replace to the first page. and continue checking
	replace_bf_page_info = bf_page_info_zeros[0];
    // for every page with fixcount zero, check with the one saved as replace page
	for (i=0; i < sizeofa(bf_page_info_zeros); i++) {
		// update intermediate variable
        next_bf_page_info = bf_page_info_zeros[i];
		// check dependeing on the strategy of replacement
        if(next_bf_page_info!=NULL){
			if(bm->strategy == RS_FIFO){
                // if next weight < replace weight ->> next page is the new replace page
				if ((replace_bf_page_info->weight) > (next_bf_page_info->weight)){
					replace_bf_page_info = next_bf_page_info;
                }
			}
			else if(bm->strategy==RS_LRU){
				// if next timestamp is newer ->> next page is the new replace page
	            if(replace_bf_page_info->timeStamp > next_bf_page_info->timeStamp){
					replace_bf_page_info = next_bf_page_info;
				}
			}
            else if (bm->strategy==RS_LFU){
                // if next weight < replace weight ->> next page is the new replace page
                if ((replace_bf_page_info->weight) > (next_bf_page_info->weight)){
                    replace_bf_page_info = next_bf_page_info;
                }
            }
		}
	}

    // return the replace page
    return replace_bf_page_info;
}



