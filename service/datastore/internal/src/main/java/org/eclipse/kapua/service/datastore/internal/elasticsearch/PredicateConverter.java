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

import java.util.HashSet;
import java.util.Set;

import org.eclipse.kapua.service.datastore.model.StorableId;
import org.eclipse.kapua.service.datastore.model.query.AndPredicate;
import org.eclipse.kapua.service.datastore.model.query.ChannelMatchPredicate;
import org.eclipse.kapua.service.datastore.model.query.IdsPredicate;
import org.eclipse.kapua.service.datastore.model.query.RangePredicate;
import org.eclipse.kapua.service.datastore.model.query.StorablePredicate;
import org.eclipse.kapua.service.datastore.model.query.TermPredicate;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;

public class PredicateConverter
{
    public QueryBuilder toElasticsearchQuery(StorablePredicate predicate) 
    		throws EsQueryConversionException
    {
        if (predicate == null)
            throw new EsQueryConversionException(String.format("Predicate parameter is undefined"));

        if (predicate instanceof AndPredicate)
            return toElasticsearchQuery((AndPredicate)predicate);

        if (predicate instanceof IdsPredicate)
            return toElasticsearchQuery((IdsPredicate)predicate);

        if (predicate instanceof ChannelMatchPredicate)
            return toElasticsearchQuery((ChannelMatchPredicate)predicate);

        if (predicate instanceof RangePredicate)
            return toElasticsearchQuery((RangePredicate)predicate);

        if (predicate instanceof TermPredicate)
            return toElasticsearchQuery((TermPredicate)predicate);

        throw new EsQueryConversionException(String.format("Unknown predicate type %s", this.getClass().getName()));
    }
    
    public QueryBuilder toElasticsearchQuery(AndPredicate predicate) 
    		throws EsQueryConversionException 
    {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        for (StorablePredicate subPredicate:predicate.getPredicates())
            boolQuery.must(this.toElasticsearchQuery(subPredicate));

        return boolQuery;
    }
    
    public QueryBuilder toElasticsearchQuery(IdsPredicate predicate) 
    		throws EsQueryConversionException 
    {
        if (predicate == null)
            throw new EsQueryConversionException(String.format("Predicate parameter is undefined"));

        Set<StorableId> ids = predicate.getIdSet();
        Set<String> stringIds = new HashSet<String>(ids.size());
        for(StorableId id:ids)
            stringIds.add(id.toString());
        
        QueryBuilder idsQuery = QueryBuilders.idsQuery(predicate.getType()).addIds(stringIds.toArray(new String[] {}));
        
        return idsQuery;
    }
    
    public QueryBuilder toElasticsearchQuery(ChannelMatchPredicate predicate) 
    		throws EsQueryConversionException
    {
        if (predicate == null)
            throw new EsQueryConversionException(String.format("Predicate parameter is undefined"));

        DatastoreChannel datastoreChannel = null;
        try 
        {
            datastoreChannel = new DatastoreChannel(predicate.getExpression());
        }
        catch (EsInvalidChannelException e)
        {
            throw new EsQueryConversionException(e);
        }

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        
        if (datastoreChannel.isAnyAccount())
            boolQuery.must(QueryBuilders.termQuery(MessageField.ACCOUNT.field(), datastoreChannel.getAccount()));

        if (datastoreChannel.isAnyClientId())
            boolQuery.must(QueryBuilders.termQuery(MessageField.CLIENT_ID.field(), datastoreChannel.getClientId()));

        if (datastoreChannel.isAnySubtopic())
            boolQuery.must(QueryBuilders.termQuery(MessageField.CHANNEL.field(), datastoreChannel.getChannel()));

        return boolQuery;
    }
    
    public QueryBuilder toElasticsearchQuery(RangePredicate predicate) 
    		throws EsQueryConversionException 
    {
        if (predicate == null)
            throw new EsQueryConversionException(String.format("Predicate parameter is undefined"));
       
        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery(predicate.getField().field());
        if (predicate.getMinValue() != null)
            rangeQuery.from(predicate.getMinValue());
        if (predicate.getMaxValue() != null)
            rangeQuery.to(predicate.getMaxValue());
       
        return rangeQuery;
    }
    
    public QueryBuilder toElasticsearchQuery(TermPredicate predicate) 
    		throws EsQueryConversionException 
    {
        if (predicate == null)
            throw new EsQueryConversionException(String.format("Predicate parameter is undefined"));
       
        TermQueryBuilder termQuery = QueryBuilders.termQuery(predicate.getField().field(), predicate.getValue());
       
        return termQuery;
    }
}
