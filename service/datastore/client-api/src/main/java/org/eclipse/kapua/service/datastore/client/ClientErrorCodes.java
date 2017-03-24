package org.eclipse.kapua.service.datastore.client;

import org.eclipse.kapua.KapuaErrorCode;

public enum ClientErrorCodes implements KapuaErrorCode {

    /**
     * Client unavailable
     */
    CLIENT_UNAVAILABLE,

    /**
     * Client undefined (not initialized or misconfigured)
     */
    CLIENT_UNDEFINED,
    /**
     * Data model mapping error
     */
    DATAMODEL_MAPPING_EXCEPTION,
    /**
     * Schema mapping error
     */
    SCHEMA_MAPPING_EXCEPTION,
    /**
     * Query mapping error
     */
    QUERY_MAPPING_EXCEPTION

}
