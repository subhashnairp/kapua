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

import org.eclipse.kapua.service.datastore.model.query.SortDirection;
import org.eclipse.kapua.service.datastore.model.query.SortField;

public class SortFieldImpl implements SortField
{

    private String        field;
    private SortDirection sortDirection;

    public SortFieldImpl()
    {

    }

    public String getField()
    {
        return field;
    }

    public void setField(String field)
    {
        this.field = field;
    }

    public SortDirection getSortDirection()
    {
        return sortDirection;
    }

    public void setSortDirection(SortDirection sortDirection)
    {
        this.sortDirection = sortDirection;
    }

}
