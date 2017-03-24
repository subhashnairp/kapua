package org.eclipse.kapua.service.datastore.client;

import java.util.Map;

public interface ModelContext {

    public <T> T unmarshal(Class<T> clazz, String serializedObject) throws DatamodelMappingException;

    public Map<String, Object> marshal(Object object) throws DatamodelMappingException;

}
