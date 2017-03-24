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

import java.util.HashMap;
import java.util.Map;

import org.eclipse.kapua.service.datastore.model.query.StorableField;
import org.eclipse.kapua.service.datastore.model.query.TermPredicate;

/**
 * Implementation of query predicate for matching field value
 * 
 * @since 1.0
 *
 */
public class TermPredicateImpl implements TermPredicate {

    private final static String PREDICATE_KEY = "term";

    private StorableField field;
    private Object value;

    /**
     * Default constructor
     */
    public TermPredicateImpl() {
    }

    /**
     * Construct a term predicate given the field and value
     * 
     * @param field
     * @param value
     */
    public <V> TermPredicateImpl(StorableField field, V value) {
        this.field = field;
        this.value = value;
    }

    @Override
    public StorableField getField() {
        return this.field;
    }

    /**
     * Return the field
     * 
     * @return
     */
    public TermPredicate setField(StorableField field) {
        this.field = field;
        return this;
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public <V> V getValue(Class<V> clazz) {
        return clazz.cast(value);
    }

    /**
     * Set the value
     * 
     * @param value
     * @return
     */
    public <V> TermPredicate setValue(V value) {
        this.value = value;
        return this;
    }

    @Override
    /**
     * <pre>
     * POST _search
     *  {
     *    "query": {
     *      "term" : { "user" : "Kimchy" } 
     *    }
     *  }
     * </pre>
     */
    public Map<String, Object> toSerializedMap() {
        Map<String, Object> outputMap = new HashMap<>();
        Map<String, Object> termMap = new HashMap<>();
        termMap.put(field.toString(), value);
        outputMap.put(PREDICATE_KEY, termMap);
        return outputMap;
    }

}
