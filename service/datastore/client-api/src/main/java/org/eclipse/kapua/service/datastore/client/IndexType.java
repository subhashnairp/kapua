package org.eclipse.kapua.service.datastore.client;


public class IndexType {

    public String id;
    public String index;

    public IndexType(String id) {
        this.id = id;
    }

    public IndexType(String id, String index) {
        this.id = id;
        this.index = index;
    }

    @Override
    public int hashCode() {
        // TODO complete the method
        // TODO Auto-generated method stub
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof IndexType) {
            IndexType indexType = (IndexType) obj;
            if (id != null) {
                return id.equals(indexType.getId());
            }
            // TODO complete the method
        }
        return false;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

}
