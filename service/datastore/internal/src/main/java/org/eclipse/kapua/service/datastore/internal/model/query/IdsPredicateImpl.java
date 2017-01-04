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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.eclipse.kapua.service.datastore.model.StorableId;
import org.eclipse.kapua.service.datastore.model.query.IdsPredicate;

public class IdsPredicateImpl implements IdsPredicate
{
    private String type;
    private Set<StorableId> idSet = new HashSet<StorableId>();

    public IdsPredicateImpl()
    {
    }

    public IdsPredicateImpl(String type)
    {
    	this();
        this.type = type;
    }

    public IdsPredicateImpl(String type, Collection<StorableId> ids)
    {
        this(type);
        this.idSet.addAll(ids);
    }

    @Override
    public String getType()
    {
        return this.type;
    }

    public IdsPredicate setType(String type)
    {
        this.type = type;
        return this;
    }

    @Override
    public Set<StorableId> getIdSet()
    {
        return this.idSet;
    }

    public IdsPredicate addValue(StorableId id)
    {
        this.idSet.add(id);
        return this;
    }

    public IdsPredicate addValues(Collection<StorableId> ids)
    {
        this.idSet.addAll(ids);
        return this;
    }

    public IdsPredicate clearValues()
    {
        this.idSet.clear();
        return this;
    }
}
