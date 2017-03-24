package org.eclipse.kapua.service.datastore.client;

public class UpdateRequest extends Request {

    private String id;

    public UpdateRequest(TypeDescriptor typeDescriptor, String id, Object storable) {
        super(typeDescriptor, storable);
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

}
