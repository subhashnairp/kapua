package org.eclipse.kapua.service.datastore.client;

import java.util.ArrayList;
import java.util.List;

public class BulkUpdateResponse {

    List<UpdateResponse> responseList;

    public BulkUpdateResponse() {
        responseList = new ArrayList<>();
    }

    public void add(UpdateResponse response) {
        responseList.add(response);
    }

    public List<UpdateResponse> getResponse() {
        return responseList;
    }

    public void setResponse(List<UpdateResponse> responseList) {
        this.responseList = responseList;
    }

}
