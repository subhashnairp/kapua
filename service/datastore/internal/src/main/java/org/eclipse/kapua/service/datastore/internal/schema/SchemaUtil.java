package org.eclipse.kapua.service.datastore.internal.schema;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.kapua.model.id.KapuaId;
import org.eclipse.kapua.service.datastore.client.DatamodelMappingException;
import org.eclipse.kapua.service.datastore.internal.mediator.DatastoreUtils;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class SchemaUtil {

    final static JsonNodeFactory factory = JsonNodeFactory.instance;

    private static final String UNSUPPORTED_OBJECT_TYPE_ERROR_MSG = "The conversion of object [%s] is not supported!";

    /**
     * Return a map of map. The contained map has, as entries, the couples subKeys-values.<br>
     * <b>NOTE! No arrays subKeys-values coherence will be done (length or null check)!</>
     * 
     * @param key
     * @param subKeys
     * @param values
     * @return
     */
    public static Map<String, Object> getMapOfMap(String key, String[] subKeys, String[] values) {
        Map<String, String> mapChildren = new HashMap<>();
        for (int i=0; i<subKeys.length; i++) {
            mapChildren.put(subKeys[i], values[i]);
        }
        Map<String, Object> map = new HashMap<>();
        map.put(key, mapChildren);
        return map;
    }

    /**
     * Get the Elasticsearch data index name
     * 
     * @param scopeId
     * @return
     */
    public static String getDataIndexName(KapuaId scopeId) {
        String scopeIdShort = scopeId.toCompactId();
        return DatastoreUtils.getDataIndexName(scopeIdShort);
    }

    /**
     * Get the Kapua data index name
     * 
     * @param scopeId
     * @return
     */
    public static String getKapuaIndexName(KapuaId scopeId) {
        String scopeIdShort = scopeId.toCompactId();
        return DatastoreUtils.getRegistryIndexName(scopeIdShort);
    }

    public static ObjectNode getField(String[] name, Object[] value) throws DatamodelMappingException {
        ObjectNode rootNode = factory.objectNode();
        for (int i = 0; i < name.length; i++) {
            if (value[i] instanceof String) {
                rootNode.set(name[i], factory.textNode((String) value[i]));
            } else if (value[i] instanceof Boolean) {
                rootNode.set(name[i], factory.booleanNode((Boolean) value[i]));
            } else if (value[i] instanceof Integer) {
                rootNode.set(name[i], factory.numberNode((Integer) value[i]));
            } else if (value[i] instanceof Long) {
                rootNode.set(name[i], factory.numberNode((Long) value[i]));
            } else if (value[i] instanceof Double) {
                rootNode.set(name[i], factory.numberNode((Double) value[i]));
            } else if (value[i] instanceof Float) {
                rootNode.set(name[i], factory.numberNode((Float) value[i]));
            } else if (value[i] instanceof byte[]) {
                rootNode.set(name[i], factory.binaryNode((byte[]) value[i]));
            } else {
                throw new DatamodelMappingException(String.format(UNSUPPORTED_OBJECT_TYPE_ERROR_MSG, value[i].getClass()));
            }
        }
        return rootNode;
    }

    public static ObjectNode getObjectNode() {
        return factory.objectNode();
    }
}
