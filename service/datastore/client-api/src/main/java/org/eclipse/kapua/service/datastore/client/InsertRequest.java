package org.eclipse.kapua.service.datastore.client;

public class InsertRequest extends Request {

    public InsertRequest(TypeDescriptor typeDescriptor, Object storable) {
        super(typeDescriptor, storable);
    }

}
