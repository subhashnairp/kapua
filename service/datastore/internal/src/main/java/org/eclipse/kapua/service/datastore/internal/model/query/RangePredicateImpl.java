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

import org.eclipse.kapua.KapuaException;
import org.eclipse.kapua.service.datastore.model.query.RangePredicate;
import org.eclipse.kapua.service.datastore.model.query.StorableField;

/**
 * Implementation of query predicate for matching range values
 * 
 * @since 1.0
 *
 */
public class RangePredicateImpl implements RangePredicate {

    private final static String PREDICATE_KEY = "range";
    private final static String GTE_KEY = "gte";
    private final static String LTE_KEY = "lte";

    private StorableField field;
    private Object minValue;
    private Object maxValue;

    private <V extends Comparable<V>> void checkRange(Class<V> clazz) throws KapuaException {
        if (minValue == null || maxValue == null)
            return;

        V min = clazz.cast(minValue);
        V max = clazz.cast(maxValue);
        if (min.compareTo(max) > 0)
            throw KapuaException.internalError("Min value must not be graeter than max value");
    }

    /**
     * Default constructor
     */
    public RangePredicateImpl() {
    }

    /**
     * Construct a range predicate given the field and the values
     * 
     * @param field
     * @param minValue
     * @param maxValue
     */
    public <V extends Comparable<V>> RangePredicateImpl(StorableField field, V minValue, V maxValue) {
        this.field = field;
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    @Override
    public StorableField getField() {
        return this.field;
    }

    /**
     * Get the field
     * 
     * @param field
     * @return
     */
    public RangePredicate setField(StorableField field) {
        this.field = field;
        return this;
    }

    @Override
    public Object getMinValue() {
        return minValue;
    }

    @Override
    public <V extends Comparable<V>> V getMinValue(Class<V> clazz) {
        return clazz.cast(minValue);
    }

    /**
     * Set the minimum value (typed)
     * 
     * @param clazz
     * @param minValue
     * @return
     * @throws KapuaException
     */
    public <V extends Comparable<V>> RangePredicate setMinValue(Class<V> clazz, V minValue) throws KapuaException {
        this.minValue = minValue;
        checkRange(clazz);
        return this;
    }

    @Override
    public Object getMaxValue() {
        return maxValue;
    }

    @Override
    public <V extends Comparable<V>> V getMaxValue(Class<V> clazz) {
        return clazz.cast(maxValue);
    }

    /**
     * Set the maximum value (typed)
     * 
     * @param clazz
     * @param maxValue
     * @return
     * @throws KapuaException
     */
    public <V extends Comparable<V>> RangePredicate setMaxValue(Class<V> clazz, V maxValue) throws KapuaException {
        this.maxValue = maxValue;
        checkRange(clazz);
        return this;
    }

    /**
     * <pre>
     * GET _search
     *  {
     *      "query": {
     *          "range" : {
     *              "age" : {
     *                  "gte" : 10,
     *                  "lte" : 20,
     *                  "boost" : 2.0
     *              }
     *          }
     *      }
     *  }
     * </pre>
     */
    public Map<String, Object> toSerializedMap() {
        Map<String, Object> outputMap = new HashMap<>();
        Map<String, Object> valuesMap = new HashMap<>();
        valuesMap.put(LTE_KEY, maxValue);
        valuesMap.put(GTE_KEY, minValue);
        Map<String, Object> termMap = new HashMap<>();
        termMap.put(field.toString(), valuesMap);
        outputMap.put(PREDICATE_KEY, termMap);
        return outputMap;
    }

}
