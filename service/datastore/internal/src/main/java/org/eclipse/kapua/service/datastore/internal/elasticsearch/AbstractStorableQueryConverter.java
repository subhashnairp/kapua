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

import org.eclipse.kapua.service.datastore.model.Storable;
import org.eclipse.kapua.service.datastore.model.query.SortDirection;
import org.eclipse.kapua.service.datastore.model.query.SortField;
import org.eclipse.kapua.service.datastore.model.query.StorableFetchStyle;
import org.eclipse.kapua.service.datastore.model.query.StorableQuery;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

public abstract class AbstractStorableQueryConverter<S extends Storable, Q extends StorableQuery<S>>
{
    public SearchRequestBuilder toCountRequestBuilder(String indices, String type, Q query)
        throws EsQueryConversionException, EsClientUnavailableException
    {
        if (query == null)
            throw new NullPointerException(String.format("Query parameter is undefined"));

        PredicateConverter pc = new PredicateConverter();
        SearchRequestBuilder searchReqBuilder = ElasticsearchClient.getInstance().prepareSearch(indices);
        searchReqBuilder.setTypes(type)
                        .setQuery(pc.toElasticsearchQuery(query.getPredicate()))
                        .setSize(0);

        return searchReqBuilder;
    }

    public SearchRequestBuilder toSearchRequestBuilder(String indices, String type, Q query)
        throws EsQueryConversionException, EsClientUnavailableException
    {
        if (query == null)
            throw new NullPointerException(String.format("Query parameter is undefined"));

        PredicateConverter pc = new PredicateConverter();
        SearchRequestBuilder searchReqBuilder = ElasticsearchClient.getInstance().prepareSearch(indices);
        searchReqBuilder.setTypes(type)
                        .setQuery(pc.toElasticsearchQuery(query.getPredicate()));
        if (query.getSortFields() != null) {
            for (SortField sf : query.getSortFields()) {
                if (sf.getSortDirection() == null) {
                    throw new NullPointerException(String.format("The order for the field [%s] is undefined!", sf.getField()));
                }
                FieldSortBuilder fsb = SortBuilders.fieldSort(sf.getField());
                if (SortDirection.ASC.equals(sf.getSortDirection())) {
                    fsb.order(SortOrder.ASC);
                }
                else {
                    fsb.order(SortOrder.DESC);
                }
                searchReqBuilder.addSort(fsb);
            }
        }
        searchReqBuilder.setFrom(query.getOffset())
                        .setSize(query.getLimit());

        String[] includes = this.getIncludes(query.getFetchStyle());
        String[] excludes = this.getExcludes(query.getFetchStyle());
        if (includes != null || excludes != null)
            searchReqBuilder.setFetchSource(includes, excludes);

        if (this.getFields() != null && this.getFields().length > 0)
            searchReqBuilder.addFields(this.getFields());

        return searchReqBuilder;
    }

    protected abstract String[] getIncludes(StorableFetchStyle fetchStyle);

    protected abstract String[] getExcludes(StorableFetchStyle fetchStyle);

    protected abstract String[] getFields();
}
