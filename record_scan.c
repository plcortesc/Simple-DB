#include "record_scan.h"

#include <stdlib.h>
#include <stdio.h>

//This method is used to start the scan
RC insert(Scan_Node_ptr *stnode , RM_ScanHandle *sHandle , AUX_Scan *auxScan)
{
    //previous node
    Scan_Node_ptr pNode=NULL;
    //current node
    Scan_Node_ptr cNode=*stnode;
    //new node in 
    Scan_Node_ptr nNode=(Scan_Node *)malloc(sizeof(Scan_Node));

    //setting the values of the new node
    nNode->sHandle=sHandle;
    nNode->auxScan=auxScan;

    while(cNode!=NULL){
        pNode=cNode;
        cNode=cNode->nextScan;
    }

    if(pNode==NULL) *stnode=nNode;
    else pNode->nextScan=nNode;

    return RC_OK;
}
//This method is used to close the scan
RC delete(Scan_Node_ptr *stnode , RM_ScanHandle *sHandle)
{
    //current node to be deleted
    Scan_Node_ptr cNode=*stnode;

    if(cNode!=NULL){
        Scan_Node_ptr temptr=cNode;
        *stnode=cNode->nextScan;
    }
    return RC_OK;
}

//This method is used to carry out the scanning and to close the scan
//returning the node we have to check or close.
AUX_Scan *search(Scan_Node_ptr stnode , RM_ScanHandle *sHandle)
{
    Scan_Node_ptr pNode = NULL;
    Scan_Node_ptr cNode = stnode;
    while(cNode!=NULL && cNode->sHandle!=sHandle){
        pNode=cNode;
        cNode=cNode->nextScan;
    }

    return cNode->auxScan;
}
