// Abdelrahman Mohammad
// Rami Elsayed 908 421 7182
// Benjamin Yang 908 476 4837


#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}



/**
 * @brief  Brief description of this function
 * 
 * Long description
 * 
 * @param  value1:  description
 * @param  value2:  description
 * 
 * @return description
 */
const Status BufMgr::allocBuf(int & frame) 
{    
    // first iteration
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* current = &bufTable[clockHand];
        
        if (!current->valid) 
        {
            frame = clockHand;
            advanceClock();
            return OK;
        }
        else 
        {
            if (current->refbit) // check if referenced recently
            {
                current->refbit = false;
                advanceClock();
                continue;
            }
            else
            {
                if (current->pinCnt > 0) continue;
                if (current->dirty) 
                {
                    Status s = current->file->writePage(current->pageNo, &bufPool[clockHand]); // write to disk
                    if (s != OK) return s;
                }
                current->Clear();
                frame = clockHand;
                advanceClock();
                return OK;
            }
        }
    }

    // second iteration
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* current = &bufTable[clockHand];
        
        if (!current->valid) 
        {
            frame = clockHand;
            advanceClock();
            return OK;
        }
        else 
        {
            if (current->refbit) // check if referenced recently
            {
                advanceClock();
                continue; // being used after first iteration
            }
            else // no longer referenced after first iteration
            {
                if (current->pinCnt > 0) continue;
                if (current->dirty) 
                {
                    Status s = current->file->writePage(current->pageNo, &bufPool[clockHand]); // write to disk
                    if (s != OK) return s;
                }
                current->Clear();
                frame = clockHand;
                advanceClock();
                return OK;
            }
        }
    }

    return BUFFEREXCEEDED;
}

	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    int frameNo = 0;
    Status found = hashTable->lookup(file, PageNo, frameNo);

    if (found == OK)
    {
        cout << "Found\n";
        BufDesc* frame = &bufTable[frameNo];
        frame->refbit = true;
        frame->pinCnt+=1;
        page = &bufPool[frameNo];
    } 
    else 
    {
        cout << "Read from disk\n";
        Status s = allocBuf(frameNo);
        
        if (s == OK)
        {
            Status readPageSuccess = file->readPage(PageNo, &bufPool[frameNo]);

            if (readPageSuccess != OK) return readPageSuccess;

            BufDesc* frame = &bufTable[frameNo];
            Status insert = hashTable->insert(file, PageNo, frameNo);
            if (insert != OK) return insert;
            frame->Set(file, PageNo);
            page = &bufPool[frameNo];
        }
        else
        {
            return s;
        }
    }
    cout << "Page content: " << (char*) page << endl;


    return OK;
}


/**
 * This function unpins pages in the buffer
 * 
 * @param file poitner to the file that contains the page
 * @param PageNo the page # to unpin
 * @param dirty if the page differs from disk (has been modified)
 * @return Status OK if successful, HASHNOTFOUND if the page is not in pool,
 *         PAGENOTPINNED if page pin is 0
 */
const Status BufMgr::unPinPage(File* file, const int PageNo, const bool dirty) {
    int frame_n;
    Status status = hashTable->lookup(file, PageNo, frame_n);
    
    if (status != OK) {
        return HASHNOTFOUND;
    }
        
    BufDesc& frame = bufTable[frame_n];
    
    if (frame.pinCnt < 1) {
        return PAGENOTPINNED;
    }   

    frame.pinCnt--;

    if (dirty) {
        frame.dirty = true;
    }
        
    return OK;
}

/**
 * This function allocates a page in the provided file. Then it allocates
 * a buffer fram from the buffer pool and updates the hashtable so that
 * the file and page number will map to the frame we just allocated.
 * We then initialize the buffer descriptor to reflect the current state
 * of the buffer frame. Once everything is done return OK
 * 
 * @param file pointer to the file that contains the page
 * @param pageNo page number to allocate
 * @param page pointer to the allocated page in the buffer pool
 */
const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{

    Status status;

    //Allocate a new page in the file and get the page number
    status = file->allocatePage(pageNo);
    if(status != OK){
        return status;  //Return error because allocation fails 
    }

    //Allocate a frame in the buffer for the new page
    int frameNo;
    status = allocBuf(frameNo);
    if(status != OK){
        return status;  //Return error because no buffer frame is available
    }

    //Insert the (file,pageNo) entry into the hash table
    status = hashTable->insert(file, pageNo, frameNo);
    if(status != OK){
        return status;  //Return an error because insertion into hash table fails
    }

    //Update the buffer desc to reflect the current state of the frame
    BufDesc& bufDesc = bufTable[frameNo];
    bufDesc.file = file;
    bufDesc.pageNo = pageNo;
    bufDesc.pinCnt = 1;
    bufDesc.dirty = true;   //We changed the buffer frame in the buffer so we need to mark it dirty
    bufDesc.valid = true;   //Buffer frame contains a valid page
    bufDesc.Set(file, pageNo);

    page = &bufPool[frameNo];

    return OK;

}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}