#include "catalog.h"
#include "query.h"

/*
 * Deletes records from a specified relation.
 *
 * Returns:
 *     OK on success
 *     an error code otherwise
 */

const Status QU_Delete(const string & relation, 
               const string & attrName, 
               const Operator op,
               const Datatype type, 
               const char *attrValue)
{
    //Open the relation
    RelDesc relDesc;
    Status status = relCat->getInfo(relation, relDesc);
    if(status != OK){
        std::cerr << "Error: Relation " << relation << " does not exist." << std::endl;
        return status;
    }

    if(attrName.empty()) {
        HeapFileScan scan(relation, status);
        if(status != OK) return status;
        
        // Scan all records and delete them
        RID rid;
        status = scan.startScan(0, 0, STRING, NULL, EQ);
        if(status != OK) return status;
        
        while(scan.scanNext(rid) == OK) {
            status = scan.deleteRecord();
            if(status != OK) {
                scan.endScan();
                return status;
            }
        }
        scan.endScan();
        return OK;
    }

    //get attributes
    AttrDesc *attributes;
    int attrCount;
    status = attrCat->getRelInfo(relation, attrCount, attributes);
    if(status != OK){
        std::cerr << "Error: Failed to retrieve attributes for relation " << relation << "." << std::endl;
        return status;
    }

    //find attribute descriptor for the given attr name
    AttrDesc *targetAttr = nullptr;
    for(int i = 0; i < attrCount; i++){
        if(strcmp(attributes[i].attrName, attrName.c_str()) == 0){
            targetAttr = &attributes[i];
            break;
        }
    }

    if(!targetAttr){//target has not been found
        std::cerr << "Error: Attribute " << attrName << " not found in relation schema." << std::endl;
        delete[] attributes;
        return ATTRNOTFOUND;
    }

    //open heap file for relation
    HeapFileScan scan(relation, status);
    if(status != OK){
        std::cerr << "Error: Could not open heap file for relation " << relation << "." << std::endl;
        delete[] attributes;
        return status;
    }

    //filtered scan
    char scanFilter[MAXSTRINGLEN];
    if (attrValue != NULL) {
        if (type == INTEGER) {
            int value = atoi(attrValue);
            memcpy(scanFilter, &value, sizeof(int));
        } else if (type == FLOAT) {
            float value = atof(attrValue);
            memcpy(scanFilter, &value, sizeof(float));
        } else {
            strncpy(scanFilter, attrValue, MAXSTRINGLEN);
        }
    }

    status = scan.startScan(targetAttr->attrOffset, targetAttr->attrLen, type, attrValue != NULL ? scanFilter : NULL, op);

    if(status != OK){
        std::cerr << "Error: Could not start a scan on the relation " << relation << "." << std::endl;
        delete[] attributes;
        return status;
    }

    RID rid;
    while(scan.scanNext(rid) == OK){
        status = scan.deleteRecord();
        if(status != OK){
            std::cerr << "Error: Could not delete record." << std::endl;
            scan.endScan();
            delete[] attributes;
            return status;
        }
    }

    scan.endScan();
    delete[] attributes;//no longer needed
    return OK;
}