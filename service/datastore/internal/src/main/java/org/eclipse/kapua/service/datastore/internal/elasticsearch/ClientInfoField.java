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
package org.eclipse.kapua.service.datastore.internal.elasticsearch;

import org.eclipse.kapua.service.datastore.model.query.StorableField;

public enum ClientInfoField implements StorableField
{
    ACCOUNT(EsSchema.CLIENT_ACCOUNT),
    CLIENT_ID(EsSchema.CLIENT_ID),
    TIMESTAMP(EsSchema.CLIENT_TIMESTAMP),
    MESSAGE_ID(EsSchema.CLIENT_MESSAGE_ID);

    private String field;

    private ClientInfoField(String name)
    {
        this.field = name;
    }

    @Override
    public String field()
    {
        return field;
    }
}
