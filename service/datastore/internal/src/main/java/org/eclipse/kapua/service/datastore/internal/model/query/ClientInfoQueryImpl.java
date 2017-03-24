/*******************************************************************************
 * Copyright (c) 2011, 2017 Eurotech and/or its affiliates and others
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *  
 * Contributors:
 *     Eurotech - initial API and implementation
 *******************************************************************************/
package org.eclipse.kapua.service.datastore.internal.model.query;

import org.eclipse.kapua.service.datastore.internal.AbstractStorableQuery;
import org.eclipse.kapua.service.datastore.internal.mediator.ClientInfoField;
import org.eclipse.kapua.service.datastore.model.ClientInfo;
import org.eclipse.kapua.service.datastore.model.query.ClientInfoQuery;
import org.eclipse.kapua.service.datastore.model.query.StorableFetchStyle;

/**
 * Client information query implementation
 * 
 * @since 1.0
 *
 */
public class ClientInfoQueryImpl extends AbstractStorableQuery<ClientInfo> implements ClientInfoQuery {

    @Override
    public String[] getIncludes(StorableFetchStyle fetchStyle) {
        return new String[] { "" };
    }

    @Override
    public String[] getExcludes(StorableFetchStyle fetchStyle) {
        return new String[] { "*" };
    }

    @Override
    public String[] getFields() {
        return new String[] { ClientInfoField.CLIENT_ID.field(),
                ClientInfoField.TIMESTAMP.field(),
                ClientInfoField.ACCOUNT.field(),
                ClientInfoField.MESSAGE_ID.field() };
    }

}
