
***USAGE***

To compile:  

 $ make

To run the first test case: 

 $ ./test_assign3 and the second one, type: ./test_expr


***IMPLEMENTATION DESIGN***


Storage Manager: Implement a storage manager that allows read/writing of blocks to/from a file on disk

Buffer Manager: Implement a buffer manager that manages a buffer of blocks in memory including reading/flushing to disk and block replacement (flushing blocks to disk to make space for reading new blocks from disk)

Record Manager: Implement a simple record manager that allows navigation through records, and inserting and deleting records
