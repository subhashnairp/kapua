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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.kapua.service.datastore.model.StorableId;
import org.eclipse.kapua.service.datastore.model.query.IdsPredicate;

/**
 * Implementation of query predicate for matching identifier values fields
 * 
 * @since 1.0
 *
 */
public class IdsPredicateImpl implements IdsPredicate {

    private final static String PREDICATE_KEY = "ids";
    private final static String TYPE_KEY = "type";
    private final static String VALUES_KEY = "values";

    private String type;
    private Set<StorableId> idSet = new HashSet<StorableId>();

    /**
     * Default constructor
     */
    public IdsPredicateImpl() {
    }

    /**
     * Construct an identifier predicate given the type
     * 
     * @param type
     */
    public IdsPredicateImpl(String type) {
        this();
        this.type = type;
    }

    /**
     * Construct an identifier predicate given the type and the identifier collection
     * 
     * @param type
     * @param ids
     */
    public IdsPredicateImpl(String type, Collection<StorableId> ids) {
        this(type);
        this.idSet.addAll(ids);
    }

    @Override
    public String getType() {
        return this.type;
    }

    /**
     * Set the identifier type
     * 
     * @param type
     * @return
     */
    public IdsPredicate setType(String type) {
        this.type = type;
        return this;
    }

    @Override
    public Set<StorableId> getIdSet() {
        return this.idSet;
    }

    /**
     * Add the storable identifier to the identifier set
     * 
     * @param id
     * @return
     */
    public IdsPredicate addValue(StorableId id) {
        this.idSet.add(id);
        return this;
    }

    /**
     * Add the storable identifier list to the identifier set
     * 
     * @param ids
     * @return
     */
    public IdsPredicate addValues(Collection<StorableId> ids) {
        this.idSet.addAll(ids);
        return this;
    }

    /**
     * Clear the storable identifier set
     * 
     * @return
     */
    public IdsPredicate clearValues() {
        this.idSet.clear();
        return this;
    }

    @Override
    /**
     * <pre>
     * GET /_search
     *  {
     *      "query": {
     *          "ids" : {
     *              "type" : "my_type",
     *              "values" : ["1", "4", "100"]
     *          }
     *      }
     *  }
     * </pre>
     */
    public Map<String, Object> toSerializedMap() {
        Map<String, Object> outputMap = new HashMap<>();
        Map<String, Object> idsMap = new HashMap<>();
        List<String> idsList = new ArrayList<>();
        for (StorableId id : idSet) {
            idsList.add(id.toString());
        }
        idsMap.put(TYPE_KEY, type);
        idsMap.put(VALUES_KEY, idsList);
        outputMap.put(PREDICATE_KEY, idsMap);
        return outputMap;
    }

}
