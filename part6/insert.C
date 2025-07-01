#include "catalog.h"
#include "query.h"

const Status QU_Insert(const string & relation, 
    const int attrCnt, 
    const attrInfo attrList[])
{
    Status status;
    
    // Input validation
    if (relation.empty() || attrCnt <= 0 || attrList == NULL) {
        return BADCATPARM;
    }

    RelDesc relRec;
    status = relCat->getInfo(relation, relRec);
    if (status != OK) return status;

    if (attrCnt != relRec.attrCnt) { // Additonal or missing attributes, do not insert
        return BADCATPARM;
    }

    AttrDesc *attrRec;
    int cnt;
    status = attrCat->getRelInfo(relation, cnt, attrRec);
    if (status != OK) return status;

    attrInfo newList[attrCnt];
    for (int i = 0; i < cnt; i++) { // loop through attributes we know are in attrCat
        int found = 0;
        for (int j = 0; j < attrCnt; j++) {
            if (strcmp(attrList[j].attrName, attrRec[i].attrName) == 0) {
                found = 1;
                newList[i] = attrList[j];
            }
        }
        if (!found) { // missing attribute
            return BADCATPARM; 
        }
    }

    int bufLen = attrRec[cnt-1].attrOffset + attrRec[cnt-1].attrLen;
    char* recBuf = new char[bufLen];
    for (int i = 0; i < cnt; i++) {
        if (attrRec[i].attrType == INTEGER) {
            int intVal = atoi((char*)newList[i].attrValue);
            memcpy(recBuf + attrRec[i].attrOffset, &intVal, attrRec[i].attrLen);
        }
        else if (attrRec[i].attrType == FLOAT) {
            float floatVal = atof((char*)newList[i].attrValue);
            memcpy(recBuf + attrRec[i].attrOffset, &floatVal, attrRec[i].attrLen);
        }
        else {  // STRING
            memcpy(recBuf + attrRec[i].attrOffset, newList[i].attrValue, attrRec[i].attrLen);
        }
    }

    InsertFileScan* scan = new InsertFileScan(relation, status);
    if (status != OK) return status;

    Record rec;
    rec.data = recBuf;
    rec.length = bufLen;

    RID outRid;
    status = scan->insertRecord(rec, outRid);

    if (status != OK) {
        delete [] recBuf;
        delete scan;
        delete [] attrRec;
        return status;
    }

    delete [] recBuf;
    delete scan;
    delete [] attrRec;

    return status;
}