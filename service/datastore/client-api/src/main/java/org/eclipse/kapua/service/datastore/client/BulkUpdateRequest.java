package org.eclipse.kapua.service.datastore.client;

import java.util.ArrayList;
import java.util.List;

public class BulkUpdateRequest {

    List<UpdateRequest> requestList;

    public BulkUpdateRequest() {
        requestList = new ArrayList<>();
    }

    public void add(UpdateRequest request) {
        requestList.add(request);
    }

    public List<UpdateRequest> getRequest() {
        return requestList;
    }

    public void setRequest(List<UpdateRequest> requestList) {
        this.requestList = requestList;
    }

}
