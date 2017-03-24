package org.eclipse.kapua.service.datastore.internal.mediator;

import org.eclipse.kapua.KapuaErrorCode;

public enum DatastoreErrorCodes implements KapuaErrorCode {

    /**
     * Wrong or missing configuration parameters
     */
    CONFIGURATION_ERROR,
    /**
     * Invalid channel format
     */
    INVALID_CHANNEL

}
