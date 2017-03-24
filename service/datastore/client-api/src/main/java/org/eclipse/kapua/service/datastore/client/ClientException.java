package org.eclipse.kapua.service.datastore.client;

import org.eclipse.kapua.KapuaErrorCode;
import org.eclipse.kapua.KapuaException;

public class ClientException extends KapuaException {

    private static final long serialVersionUID = 2393001020208113850L;

    public ClientException(KapuaErrorCode code) {
        super(code);
    }

    public ClientException(KapuaErrorCode code, String msg) {
        super(code, msg);
    }

    public ClientException(KapuaErrorCode code, Throwable t, String msg) {
        super(code, t, msg);
    }

}
