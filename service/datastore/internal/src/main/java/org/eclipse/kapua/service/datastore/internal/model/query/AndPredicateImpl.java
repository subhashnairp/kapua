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
import java.util.List;
import java.util.Map;

import org.eclipse.kapua.service.datastore.model.query.AndPredicate;
import org.eclipse.kapua.service.datastore.model.query.StorablePredicate;

/**
 * Implementation of query "and" aggregation
 * 
 * @since 1.0
 *
 */
public class AndPredicateImpl implements AndPredicate {

    private final static String PREDICATE_KEY = "bool";
    private final static String MUST_KEY = "must";

    private List<StorablePredicate> predicates = new ArrayList<StorablePredicate>();

    /**
     * Default constructor
     */
    public AndPredicateImpl() {
    }

    /**
     * Creates an and predicate for the given predicates collection
     * 
     * @param predicates
     */
    public AndPredicateImpl(Collection<StorablePredicate> predicates) {
        predicates.addAll(predicates);
    }

    @Override
    public List<StorablePredicate> getPredicates() {
        return this.predicates;
    }

    /**
     * Add the storable predicate to the predicates collection
     * 
     * @param predicate
     * @return
     */
    public AndPredicate addPredicate(StorablePredicate predicate) {
        this.predicates.add(predicate);
        return this;

    }

    /**
     * Clear the predicates collection
     * 
     * @return
     */
    public AndPredicate clearPredicates() {
        this.predicates.clear();
        return this;
    }

    @Override
    /**
     * <pre>
     * {
     *  "query": {
     *      "bool" : {
     *        "must" : {
     *          "term" : { "user" : "kimchy" }
     *        },
     *        "filter": {
     *          "term" : { "tag" : "tech" }
     *        },
     *        "must_not" : {
     *          "range" : {
     *            "age" : { "from" : 10, "to" : 20 }
     *          }
     *        },
     *        "should" : [
     *          { "term" : { "tag" : "wow" } },
     *          { "term" : { "tag" : "elasticsearch" } }
     *        ],
     *        "minimum_should_match" : 1,
     *        "boost" : 1.0
     *      }
     *  }
     *}
     * </pre>
     */
    public Map<String, Object> toSerializedMap() {
        Map<String, Object> outputMap = new HashMap<>();
        Map<String, Object> boolMap = new HashMap<>();
        for (StorablePredicate predicate : predicates) {
            boolMap.put(MUST_KEY, predicate.toSerializedMap());
        }
        outputMap.put(PREDICATE_KEY, boolMap);
        return outputMap;
    }

}
