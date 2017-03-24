package org.eclipse.kapua.service.datastore.client;

public class UpdateResponse {

    /**
     * Record id (it should set by the driver component)
     */
    private String id;
    /**
     * Schema and index name
     */
    private TypeDescriptor typeDescriptor;
    /**
     * Result flag (true if the operation completed successfully)
     */
    private boolean result;
    /**
     * Result description
     */
    private String description;
    /**
     * Update exception
     */
    private Throwable t;

    /**
     * Positive result constructor (result true)
     * 
     * @param id
     * @param typeDescriptor
     */
    public UpdateResponse(String id, TypeDescriptor typeDescriptor) {
        this.id = id;
        this.typeDescriptor = typeDescriptor;
        result = true;
    }

    /**
     * Negative result constructor (result false)
     * 
     * @param id
     * @param typeDescriptor
     * @param description
     */
    public UpdateResponse(String id, TypeDescriptor typeDescriptor, String description) {
        this(id, typeDescriptor);
        this.description = description;
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

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Throwable getT() {
        return t;
    }

    public void setT(Throwable t) {
        this.t = t;
    }

}
