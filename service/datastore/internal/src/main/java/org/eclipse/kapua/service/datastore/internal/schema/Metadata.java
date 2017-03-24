package org.eclipse.kapua.service.datastore.internal.schema;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.kapua.service.datastore.internal.mediator.Metric;

/**
 * Metadata object
 * 
 * @since 1.0
 *
 */
public class Metadata {

    // Info fields does not change within the same account name
    private String dataIndexName;
    private String registryIndexName;
    //

    // Custom mappings can only increase within the same account
    // No removal of existing cached mappings or changes in the
    // existing mappings.
    private Map<String, Metric> messageMappingsCache;
    //

    public Map<String, Metric> getMessageMappingsCache() {
        return messageMappingsCache;
    }

    /**
     * Contruct metadata
     */
    public Metadata(String dataIndexName, String registryIndexName) {
        messageMappingsCache = new HashMap<String, Metric>(100);
        this.dataIndexName = dataIndexName;
        this.registryIndexName = registryIndexName;
    }

    /**
     * Get the Elasticsearch data index name
     * 
     * @return
     */
    public String getDataIndexName() {
        return dataIndexName;
    }

    /**
     * Get the Kapua data index name
     * 
     * @return
     */
    public String getRegistryIndexName() {
        return registryIndexName;
    }
}