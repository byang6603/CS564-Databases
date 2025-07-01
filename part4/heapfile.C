#include "heapfile.h"
#include "error.h"

// routine to create a heapfile
const Status createHeapFile(const string fileName)
{
    File*         file;
    Status         status;
    FileHdrPage*    hdrPage;
    int            hdrPageNo;
    int            newPageNo;
    Page*        newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
    {
        // file doesn't exist. First create it and allocate
        // an empty header page and data page.
        status = db.createFile(fileName);
        if (status != OK) { // creation failed
            cout << "Failed to create file" << endl;
            return status;
        }

        status = db.openFile(fileName, file); // fetch a pointer to the newly created file
        if (status != OK) { // openning new file failed
            cout << "Failed to open newly created file" << endl;
            return status;
        }

        hdrPageNo = 0;
        status = bufMgr->allocPage(file, hdrPageNo, newPage);
        if (status != OK) { // failed to allocPage
            cout << "Failed to allocPage for header" << endl;
            db.closeFile(file);
            return status;
        }
        hdrPage = (FileHdrPage*) newPage;

        newPageNo = 1;
        status = bufMgr->allocPage(file, newPageNo, newPage);
        if (status != OK) { // failed to allocPage
            cout << "Failed to allocPage for newPage (intiital page)" << endl;
            bufMgr->unPinPage(file, hdrPageNo, false); // allow page to be removed from memory
            db.closeFile(file);
            return status;
        }
        newPage->init(newPageNo);

        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;
        hdrPage->recCnt = 0;
        hdrPage->pageCnt = 1;

        status = bufMgr->unPinPage(file, hdrPageNo, true);
        if (status != OK) {
            db.closeFile(file);
            return status;
        }
        status = bufMgr->unPinPage(file, newPageNo, true);
        if (status != OK) {
            db.closeFile(file);
            return status;
        }

        db.closeFile(file); // don't need open anymore
        return OK;
    }
    return (FILEEXISTS);
}
// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}

// constructor opens the underlying file
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status  status;
    Page*   pagePtr;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
        // Get the header page number by reading through file
        headerPageNo = 0;
        filePtr->getFirstPage(headerPageNo);
        if(headerPageNo == -1){
            cerr << "Error: Invalid Page Number\n";
            return;
        }

        // Read and pin the header page
        if((status = bufMgr->readPage(filePtr, headerPageNo, pagePtr)) != OK){
            cerr << "Error: Failed to pin page number";
            returnStatus = status;
            return;
        }
        
        headerPage = (FileHdrPage*) pagePtr;
        hdrDirtyFlag = false;

        // Get the first data page
        curPageNo = headerPage->firstPage;
        if(curPageNo == -1){
            curPage = nullptr;
            curDirtyFlag = false;
            curRec = NULLRID;
            returnStatus = OK;
            return;
        }

        // Read and pin the first data page
        if((status = bufMgr->readPage(filePtr, curPageNo, pagePtr)) != OK){
            cerr << "Error: Failed to pin first data page\n";
            bufMgr->unPinPage(filePtr, headerPageNo, false);
            headerPage = nullptr;
            returnStatus = status;
            return;
        }
        curPage = pagePtr;
        curDirtyFlag = false;
        curRec = NULLRID;
        returnStatus = OK;
    }
    else
    {
        cerr << "open of heap file failed\n";
        returnStatus = status;
        return;
    }
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
    	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		if (status != OK) cerr << "error in unpin of date page\n";
    }
	
	 // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";
	
	// status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
	// if (status != OK) cerr << "error in flushFile call\n";
	// before close the file
	status = db.closeFile(filePtr);
    if (status != OK)
    {
		cerr << "error in closefile call\n";
		Error e;
		e.print (status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;
    
    if (curPage == NULL || curPageNo != rid.pageNo) 
    {
        if (curPage != NULL) 
        {
            // unpin
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK) return status;
        }
        
        status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
        if (status != OK) return status;
        
        curPageNo = rid.pageNo;
        curDirtyFlag = false;
    }
    
    status = curPage->getRecord(rid, rec);
    if (status != OK) return status;
    
    curRec = rid;
    return OK;
}

HeapFileScan::HeapFileScan(const string & name,
			   Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
				     const int length_,
				     const Datatype type_, 
				     const char* filter_,
				     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }
    
    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int)
         || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}


const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
		curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo) 
    {
		if (curPage != NULL)
		{
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;
		}
		// restore curPageNo and curRec values
		curPageNo = markedPageNo;
		curRec = markedRec;
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}


const Status HeapFileScan::scanNext(RID& outRid)
{
    Status     status = OK;
    RID        nextRid;
    RID        tmpRid;
    int     nextPageNo;
    Record      rec;

    // inherited 
    if (curPage == NULL) return BADPAGEPTR; 

    while (true) 
    {
        if (status != OK) return status;
        // move to next page if at end of page
        if (status == ENDOFPAGE) {
            status = curPage->getNextPage(nextPageNo);
            if (status != OK) return status;

            if (nextPageNo == -1) return FILEEOF;

            // unpin the page since we dojn't need it    
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK) return status;

            // read next page
            curPageNo = nextPageNo;
            status = bufMgr->readPage(filePtr, curPageNo, curPage);
            if (status != OK) return status;
            curDirtyFlag = false;

            // get first record of new page
            status = curPage->firstRecord(tmpRid);
            if (status != OK) return status;
        }
        // get record to match against filter
        status = curPage->getRecord(tmpRid, rec);
        if (status != OK) return status;

        // if matches
        if (!filter || matchRec(rec)) {
            curRec = tmpRid;
            outRid = curRec; // return next RID here
            return OK;
        }

        // otherwise, keep moving
        status = curPage->nextRecord(tmpRid, nextRid);
        tmpRid = nextRid;
    }
}


// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file. 
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true; 
    return status;
}


// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
	return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

    case INTEGER:
        int iattr, ifltr;                 // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;               // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
  //Do nothing. Heapfile constructor will bread the header page and the first
  // data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*    newPage;
    int      newPageNo;
    Status   status;
    RID      rid;

    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        return INVALIDRECLEN;
    }

    // if no curPage is set, we continue from the last page
    if (curPage == NULL)
    {
        curPageNo = headerPage->lastPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) return status;
        curDirtyFlag = false;
    }

    // insert record into current page
    status = curPage->insertRecord(rec, rid);
    
    // allocate new page if page is full
    if (status == NOSPACE)
    {
        status = bufMgr->allocPage(filePtr, newPageNo, newPage);
        if (status != OK) return status;
        
        newPage->init(newPageNo);
        
        status = curPage->setNextPage(newPageNo);
        if (status != OK) return status;
        
        // unpin cur page since we're done with it
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK) return status;
        
        // updated headers
        headerPage->lastPage = newPageNo;
        headerPage->pageCnt++;
        hdrDirtyFlag = true;
        
        // updated pointers
        curPage = newPage;
        curPageNo = newPageNo;
        curDirtyFlag = false;
        
        // insert in new page
        status = curPage->insertRecord(rec, rid);
        if (status != OK) return status;
    }
    else if (status != OK)
    {
        return status;
    }

    // update stuff
    headerPage->recCnt++;
    hdrDirtyFlag = true;
    curDirtyFlag = true;
    curRec = rid;
    outRid = rid;

    return OK;
}