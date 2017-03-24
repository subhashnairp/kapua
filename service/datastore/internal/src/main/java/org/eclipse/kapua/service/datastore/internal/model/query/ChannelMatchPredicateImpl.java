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

import org.eclipse.kapua.service.datastore.model.query.ChannelMatchPredicate;

/**
 * Implementation of query predicate for matching the channel value
 * 
 * @since 1.0
 *
 */
public class ChannelMatchPredicateImpl implements ChannelMatchPredicate {

    private final static String PREDICATE_KEY = "bool";
    private final static String MUST_KEY = "must";

    private String field;
    private String expression;

    /**
     * Construct a channel match predicate for the given expression
     * 
     * @param field
     *            the field name
     * @param expression
     *            the channel expression (may use wildcard)
     */
    public ChannelMatchPredicateImpl(String field, String expression) {
        this.field = field;
        this.expression = expression;
    }

    @Override
    public String getExpression() {
        return this.expression;
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
        Map<String, Object> expressionMap = new HashMap<>();
        expressionMap.put(field, expression);
        Map<String, Object> boolMap = new HashMap<>();
        boolMap.put(MUST_KEY, expressionMap);
        outputMap.put(PREDICATE_KEY, boolMap);
        return outputMap;
    }

}
