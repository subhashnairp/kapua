package org.eclipse.kapua.service.datastore.client;

public abstract class Request {

    private TypeDescriptor typeDescriptor;
    private Object storable;

    protected Request(TypeDescriptor typeDescriptor, Object storable) {
        this.typeDescriptor = typeDescriptor;
        this.storable = storable;
    }

    public TypeDescriptor getTypeDescriptor() {
        return typeDescriptor;
    }

    public void setTypeDescriptor(TypeDescriptor typeDescriptor) {
        this.typeDescriptor = typeDescriptor;
    }

    public Object getStorable() {
        return storable;
    }

    public void setStorable(Object storable) {
        this.storable = storable;
    }

}
