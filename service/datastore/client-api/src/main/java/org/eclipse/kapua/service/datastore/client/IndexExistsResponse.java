package org.eclipse.kapua.service.datastore.client;


public class IndexExistsResponse {

    private boolean indexExists;
    
    public IndexExistsResponse(boolean indexExists) {
        this.indexExists = indexExists;
    }
    
    public boolean isIndexExists() {
        return indexExists;
    }

}
