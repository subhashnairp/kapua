package org.eclipse.kapua.service.datastore.client;


public class InsertResponse {

    /**
     * Record id (it should set by the datastore client component)
     */
    private String id;
    /**
     * Schema and index name
     */
    private TypeDescriptor typeDescriptor;
    private boolean result;

    public InsertResponse(String id, TypeDescriptor typeDescriptor) {
        this.id = id;
        this.typeDescriptor = typeDescriptor;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public TypeDescriptor getTypeDescriptor() {
        return typeDescriptor;
    }

    public void setTypeDescriptor(TypeDescriptor typeDescriptor) {
        this.typeDescriptor = typeDescriptor;
    }

    public boolean isResult() {
        return result;
    }

    public void setResult(boolean result) {
        this.result = result;
    }

}
