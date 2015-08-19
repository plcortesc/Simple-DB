#ifndef RECORD_SCAN_H_INCLUDED
#define RECORD_SCAN_H_INCLUDED

//new
#include "dt.h"


#include "buffer_mgr.h"
#include "record_mgr.h"
#include <stdio.h>
#include <stdlib.h>

//structure we define for the auxiliary scan
typedef struct AUX_Scan
{
    int _sPage;
    int _slotID;
    int _recLength;
    int _recsPage;
    int _numPages;
    BM_PageHandle *pHandle;

}AUX_Scan;

//structure defined to handle a scan node
typedef struct Scan_Node
{
    RM_ScanHandle *sHandle;
    AUX_Scan *auxScan;
    struct Scan_Node *nextScan;

} Scan_Node,*Scan_Node_ptr;

//auxiliary methods to deal with the scan linked list
RC insert(Scan_Node_ptr *stnode , RM_ScanHandle *sHandle , AUX_Scan *auxScan);
RC delete(Scan_Node_ptr *stnode , RM_ScanHandle *sHandle );
AUX_Scan *search(Scan_Node_ptr stnode , RM_ScanHandle *sHandle);

#endif // RECORD_SCAN_H_INCLUDED
