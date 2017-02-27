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
package org.eclipse.kapua.service.datastore.internal.elasticsearch.dao;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.kapua.service.datastore.internal.elasticsearch.ClientInfoObjectBuilder;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.ClientInfoQueryConverter;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.ClientInfoXContentBuilder;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsClientUnavailableException;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsDocumentBuilderException;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsDatastoreException;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsObjectBuilderException;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsQueryConversionException;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsSchema;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsUtils;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.PredicateConverter;
import org.eclipse.kapua.service.datastore.internal.model.ClientInfoListResultImpl;
import org.eclipse.kapua.service.datastore.internal.model.query.ClientInfoQueryImpl;
import org.eclipse.kapua.service.datastore.model.ClientInfo;
import org.eclipse.kapua.service.datastore.model.ClientInfoListResult;
import org.eclipse.kapua.service.datastore.model.query.ClientInfoQuery;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

public class EsClientInfoDAO
{

    private EsTypeDAO esTypeDAO;

    private EsClientInfoDAO()
    {}

    public EsClientInfoDAO setListener(EsDaoListener daoListener)
        throws EsDatastoreException
    {
        this.esTypeDAO.setListener(daoListener);
        return this;
    }

    public EsClientInfoDAO unsetListener(EsDaoListener daoListener)
        throws EsDatastoreException
    {
        this.esTypeDAO.unsetListener(daoListener);
        return this;
    }

    public static EsClientInfoDAO client(Client client)
    {
        EsClientInfoDAO esClientInfoDAO = new EsClientInfoDAO();
        esClientInfoDAO.esTypeDAO = EsTypeDAO.client(client);
        return esClientInfoDAO;
    }

    public EsClientInfoDAO index(String indexName)
    {
        this.esTypeDAO.type(indexName, EsSchema.CLIENT_TYPE_NAME);
        return this;
    }

    public UpdateResponse upsert(ClientInfo clientInfo)
        throws EsDocumentBuilderException
    {
        ClientInfoXContentBuilder clientInfoBuilder = new ClientInfoXContentBuilder().build(clientInfo);
        return this.esTypeDAO.upsert(clientInfoBuilder.getClientId(), clientInfoBuilder.getClientBuilder());
    }

    public UpdateResponse upsert(String id, XContentBuilder esClient)
    {
        return this.esTypeDAO.upsert(id, esClient);
    }

    public UpdateResponse update(ClientInfo clientInfo)
        throws EsDocumentBuilderException
    {
        ClientInfoXContentBuilder clientInfoBuilder = new ClientInfoXContentBuilder().build(clientInfo);
        return this.esTypeDAO.upsert(clientInfoBuilder.getClientId(), clientInfoBuilder.getClientBuilder());
    }

    public UpdateResponse update(String id, XContentBuilder esClient)
    {
        return this.esTypeDAO.update(id, esClient);
    }

    public void deleteById(String id)
    {

        esTypeDAO.getClient().prepareDelete()
                 .setIndex(esTypeDAO.getIndexName())
                 .setType(esTypeDAO.getTypeName())
                 .setId(id)
                 .get(TimeValue.timeValueMillis(EsUtils.getQueryTimeout()));
    }

    public void deleteByQuery(ClientInfoQuery query)
        throws EsQueryConversionException
    {
        PredicateConverter pc = new PredicateConverter();
        this.esTypeDAO.deleteByQuery(pc.toElasticsearchQuery(query.getPredicate()));
    }
    //
    // public void deleteByAccount(long start, long end)
    // {
    // this.deleteByQuery(this.getQueryByAssetAndDate(KapuaTopic.SINGLE_LEVEL_WCARD, true, start, end));
    // }
    //
    // public void deleteByAsset(String asset, boolean isAnyAsset)
    // {
    // this.deleteByQuery(this.getQueryByAsset(asset, isAnyAsset));
    // }

    public ClientInfoListResult query(ClientInfoQuery query)
        throws EsQueryConversionException,
        EsClientUnavailableException,
        EsObjectBuilderException
    {
        ClientInfoQueryImpl localQuery = new ClientInfoQueryImpl();
        localQuery.copy(query);

        // get one plus (if there is one) to later get the next key value
        localQuery.setLimit(query.getLimit() + 1);

        ClientInfoQueryConverter aic = new ClientInfoQueryConverter();
        SearchRequestBuilder builder = aic.toSearchRequestBuilder(esTypeDAO.getIndexName(), esTypeDAO.getTypeName(), localQuery);
        SearchResponse response = builder.get(TimeValue.timeValueMillis(EsUtils.getQueryTimeout()));
        SearchHits searchHits = response.getHits();

        if (searchHits == null || searchHits.getTotalHits() == 0)
            return new ClientInfoListResultImpl();

        int i = 0;
        long searchHitsSize = searchHits.getHits().length;

        List<ClientInfo> clientInfos = new ArrayList<ClientInfo>();
        ClientInfoObjectBuilder clientInfoBuilder = new ClientInfoObjectBuilder();
        for (SearchHit searchHit : searchHits.getHits()) {
            if (i < query.getLimit()) {
                ClientInfo clientInfo = clientInfoBuilder.build(searchHit).getClientInfo();
                clientInfos.add(clientInfo);
            }
            i++;
        }

        // TODO check equivalence with CX
        // TODO what is this nextKey
        Object nextKey = null;
        if (searchHits.getTotalHits() > query.getLimit()) {
            nextKey = query.getLimit();
        }

        ClientInfoListResultImpl result = new ClientInfoListResultImpl(nextKey, searchHitsSize);
        result.addAll(clientInfos);

        return result;
    }

    public long count(ClientInfoQuery query)
        throws EsQueryConversionException, EsClientUnavailableException
    {
        ClientInfoQueryConverter converter = new ClientInfoQueryConverter();
        SearchRequestBuilder builder = converter.toSearchRequestBuilder(esTypeDAO.getIndexName(), esTypeDAO.getTypeName(), query);
        SearchResponse response = builder.get(TimeValue.timeValueMillis(EsUtils.getQueryTimeout()));
        SearchHits searchHits = response.getHits();

        if (searchHits == null)
            return 0;

        return searchHits.getTotalHits();
    }
    //
    // public SearchHits findByAccount(String name,
    // int offset,
    // int size)
    // {
    //
    // long timeout = EsUtils.getQueryTimeout();
    //
    // SearchResponse response = esTypeDAO.getClient().prepareSearch(esTypeDAO.getIndexName())
    // .setTypes(esTypeDAO.getTypeName())
    // .setFetchSource(false)
    // .addFields(EsSchema.ASSET_NAME,
    // EsSchema.ASSET_TIMESTAMP,
    // EsSchema.ASSET_ACCOUNT)
    // .setFrom(offset)
    // .setSize(size)
    // .get(TimeValue.timeValueMillis(timeout));
    //
    // SearchHits searchHits = response.getHits();
    // return searchHits;
    // }
    //
    // public SearchHits findByAsset(String name,
    // boolean isAnyAccount,
    // String asset,
    // boolean isAnyAsset,
    // int offset,
    // int size)
    // {
    //
    // long timeout = EsUtils.getQueryTimeout();
    //
    // BoolQueryBuilder boolQuery = this.getQueryByAsset(asset, isAnyAsset);
    //
    // SearchResponse response = esTypeDAO.getClient().prepareSearch(esTypeDAO.getIndexName())
    // .setTypes(esTypeDAO.getTypeName())
    // .setFetchSource(false)
    // .addFields(EsSchema.ASSET_NAME,
    // EsSchema.ASSET_TIMESTAMP,
    // EsSchema.ASSET_ACCOUNT)
    // .setQuery(boolQuery)
    // .setFrom(offset)
    // .setSize(size)
    // .get(TimeValue.timeValueMillis(timeout));
    //
    // SearchHits searchHits = response.getHits();
    // return searchHits;
    // }
}
