package org.eclipse.kapua.service.datastore.client;


public class IndexExistsRequest {

    private String index;

    public IndexExistsRequest(String index) {
        this.index = index;
    }

    public String getIndex() {
        return index;
    }

}
