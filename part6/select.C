#include "catalog.h"
#include "query.h"

// forward declaration
const Status ScanSelect(const string & result, 
          const int projCnt, 
          const AttrDesc projNames[],
          const AttrDesc *attrDesc, 
          const Operator op, 
          const char *filter,
          const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 *  OK on success
 *  an error code otherwise
 */
const Status QU_Select(const string & result, 
              const int projCnt, 
              const attrInfo projNames[],
              const attrInfo *attr, 
              const Operator op, 
              const char *attrValue)
{
    // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;
    
    Status status;
    AttrDesc *attrs = new AttrDesc[projCnt];
    AttrDesc attrDesc;
    int reclen = 0;
    
    // To go from attrInfo to attrDesc, need to consult the catalog
    for (int i = 0; i < projCnt; i++) {
        status = attrCat->getInfo(projNames[i].relName, 
                                 projNames[i].attrName,
                                 attrs[i]);
        if (status != OK) {
            delete [] attrs;
            return status;
        }
        reclen += attrs[i].attrLen;
    }
    
    if (attr != NULL) {
        status = attrCat->getInfo(attr->relName,
                                 attr->attrName,
                                 attrDesc);
        if (status != OK) {
            delete [] attrs;
            return status;
        }
    }
    
    // Make sure to give ScanSelect the proper input
    status = ScanSelect(result,
                       projCnt,
                       attrs,
                       (attr == NULL) ? NULL : &attrDesc,
                       op,
                       attrValue,
                       reclen);
    
    delete [] attrs;
    return status;
}

const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
          const int projCnt, 
          const AttrDesc projNames[],
          const AttrDesc *attrDesc, 
          const Operator op, 
          const char *filter,
          const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;
    
    Status status;
    RID rid;
    Record rec;
    
    // have a temporary record for output table
    char *recData = new char[reclen];
    Record outputRec;
    outputRec.data = (void *) recData;
    outputRec.length = reclen;
    
    // open "result" as an InsertFileScan object
    InsertFileScan outputScan(result, status);
    if (status != OK) {
        delete [] recData;
        return status;
    }
    
    // open current table (to be scanned) as a HeapFileScan object
    HeapFileScan scan(projNames[0].relName, status);
    if (status != OK) {
        delete [] recData;
        return status;
    }
    
    // check if an unconditional scan is required
    if (attrDesc == NULL) {
        status = scan.startScan(0, 0, STRING, NULL, EQ);
    } else {
        // check attrType: INTEGER, FLOAT, STRING
        if (filter != NULL) {
            char scanFilter[MAXSTRINGLEN];
            if (attrDesc->attrType == INTEGER) {
                int value = atoi(filter);
                memcpy(scanFilter, &value, sizeof(int));
            } else if (attrDesc->attrType == FLOAT) {
                float value = atof(filter);
                memcpy(scanFilter, &value, sizeof(float));
            } else {
                strncpy(scanFilter, filter, MAXSTRINGLEN);
            }
            
            status = scan.startScan(attrDesc->attrOffset,
                                  attrDesc->attrLen,
                                  (Datatype)attrDesc->attrType,
                                  scanFilter,
                                  op);
        } else {
            status = scan.startScan(attrDesc->attrOffset,
                                  attrDesc->attrLen,
                                  (Datatype)attrDesc->attrType,
                                  NULL,
                                  op);
        }
    }
    
    if (status != OK) {
        delete [] recData;
        return status;
    }
    
    // scan the current table
    while (scan.scanNext(rid) == OK) {
        status = scan.getRecord(rec);
        if (status != OK) break;
        
        // if find a record, then copy stuff over to the temporary record (memcpy)
        int outputOffset = 0;
        for (int i = 0; i < projCnt; i++) {
            memcpy(recData + outputOffset,
                   (char*)rec.data + projNames[i].attrOffset,
                   projNames[i].attrLen);
            outputOffset += projNames[i].attrLen;
        }
        
        // insert into the output table
        RID outRID;
        status = outputScan.insertRecord(outputRec, outRID);
        if (status != OK) break;
    }
    
    delete [] recData;
    return status;
}