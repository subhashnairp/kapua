package org.eclipse.kapua.service.datastore.client.transport;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.eclipse.kapua.service.datastore.client.BulkUpdateRequest;
import org.eclipse.kapua.service.datastore.client.BulkUpdateResponse;
import org.eclipse.kapua.service.datastore.client.ClientException;
import org.eclipse.kapua.service.datastore.client.ClientUnavailableException;
import org.eclipse.kapua.service.datastore.client.ClientUndefinedException;
import org.eclipse.kapua.service.datastore.client.IndexExistsRequest;
import org.eclipse.kapua.service.datastore.client.IndexExistsResponse;
import org.eclipse.kapua.service.datastore.client.InsertRequest;
import org.eclipse.kapua.service.datastore.client.InsertResponse;
import org.eclipse.kapua.service.datastore.client.ModelContext;
import org.eclipse.kapua.service.datastore.client.QueryConverter;
import org.eclipse.kapua.service.datastore.client.TypeDescriptor;
import org.eclipse.kapua.service.datastore.client.UpdateRequest;
import org.eclipse.kapua.service.datastore.client.UpdateResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.hash.Hashing;

public class DatastoreTransportClient implements org.eclipse.kapua.service.datastore.client.Client {

    private static final Logger logger = LoggerFactory.getLogger(DatastoreTransportClient.class);

    private static final String CLIENT_UNDEFINED_MSG = "Elasticsearch client must be not null";

    private org.elasticsearch.client.Client client;
    private ModelContext modelContext;
    private QueryConverter queryConverter;

    public DatastoreTransportClient() throws ClientUnavailableException {
        client = ElasticsearchClient.getInstance();
    }

    public DatastoreTransportClient(ModelContext modelContext, QueryConverter queryConverter) throws ClientUnavailableException {
        client = ElasticsearchClient.getInstance();
        this.modelContext = modelContext;
        this.queryConverter = queryConverter;
    }

    @Override
    public void setModelContext(ModelContext modelContext) {
        this.modelContext = modelContext;
    }

    @Override
    public void setQueryConverter(QueryConverter queryConverter) {
        this.queryConverter = queryConverter;
    }

    @Override
    public InsertResponse insert(InsertRequest insertRequest) throws ClientException {
        checkClient();
        Map<String, Object> storableMap = modelContext.marshal(insertRequest.getStorable());
        IndexRequest idxRequest = new IndexRequest(insertRequest.getTypeDescriptor().getIndex(), insertRequest.getTypeDescriptor().getType()).source(storableMap);
        IndexResponse response = client.index(idxRequest).actionGet(TimeValue.timeValueMillis(getQueryTimeout()));
        return new InsertResponse(response.getId(), insertRequest.getTypeDescriptor());
    }

    @Override
    public UpdateResponse upsert(UpdateRequest upsertRequest) throws ClientException {
        checkClient();
        Map<String, Object> storableMap = modelContext.marshal(upsertRequest.getStorable());
        IndexRequest idxRequest = new IndexRequest(upsertRequest.getTypeDescriptor().getIndex(), upsertRequest.getTypeDescriptor().getType(), upsertRequest.getId()).source(storableMap);
        org.elasticsearch.action.update.UpdateRequest updateRequest = new org.elasticsearch.action.update.UpdateRequest(upsertRequest.getTypeDescriptor().getIndex(),
                upsertRequest.getTypeDescriptor().getType(), upsertRequest.getId()).doc(storableMap.toString());
        org.elasticsearch.action.update.UpdateResponse response = client.update(updateRequest.upsert(idxRequest)).actionGet(TimeValue.timeValueMillis(getQueryTimeout()));
        return new UpdateResponse(response.getId(), upsertRequest.getTypeDescriptor());
    }

    @Override
    public BulkUpdateResponse upsert(BulkUpdateRequest bulkUpsertRequest) throws ClientException {
        checkClient();
        BulkRequest bulkRequest = new BulkRequest();
        for (UpdateRequest upsertRequest : bulkUpsertRequest.getRequest()) {
            String type = upsertRequest.getTypeDescriptor().getType();
            String index = upsertRequest.getTypeDescriptor().getIndex();
            String id = upsertRequest.getId();
            Map<String, Object> mappedObject = modelContext.marshal(upsertRequest.getStorable());
            IndexRequest idxRequest = new IndexRequest(index, type, id).source(mappedObject);
            org.elasticsearch.action.update.UpdateRequest updateRequest = new org.elasticsearch.action.update.UpdateRequest(index, type, id).doc(mappedObject);
            updateRequest.upsert(idxRequest);
            bulkRequest.add(updateRequest);
        }
        
        BulkResponse bulkResponse = client.bulk(bulkRequest).actionGet(TimeValue.timeValueMillis(getQueryTimeout()));

        BulkUpdateResponse response = new BulkUpdateResponse();
        BulkItemResponse[] itemResponses = bulkResponse.getItems();
        if (itemResponses != null) {
            for (BulkItemResponse bulkItemResponse : itemResponses) {
                String metricId = ((org.elasticsearch.action.update.UpdateResponse) bulkItemResponse.getResponse()).getId();
                String indexName = bulkItemResponse.getIndex();
                String typeName = bulkItemResponse.getType();
                if (bulkItemResponse.isFailed()) {
                    String failureMessage = bulkItemResponse.getFailureMessage();
                    response.add(new UpdateResponse(metricId, new TypeDescriptor(indexName, typeName), failureMessage));
                    logger.trace(String.format("Upsert failed [%s, %s, %s]",
                            indexName, typeName, failureMessage));
                    continue;
                }
                response.add(new UpdateResponse(metricId, new TypeDescriptor(indexName, typeName)));
                logger.debug(String.format("Upsert on channel metric succesfully executed [%s.%s, %s]",
                        indexName, typeName, metricId));
            }
        }
        return response;
    }

    @Override
    public <T> T find(TypeDescriptor typeDescriptor, Object query, Class<T> clazz) throws ClientException {
        List<T> result = query(typeDescriptor, query, clazz);
        if (result == null || result.size() == 0) {
            return null;
        } else {
            return result.get(0);
        }
    }

    @Override
    public <T> List<T> query(TypeDescriptor typeDescriptor, Object query, Class<T> clazz) throws ClientException {
        checkClient();
        Map<String, Object> queryMap = queryConverter.convertQuery(query);
        SearchRequestBuilder searchReqBuilder = client.prepareSearch(typeDescriptor.getIndex());
        searchReqBuilder.setTypes(typeDescriptor.getType())
                .setQuery((Map<?, ?>) queryMap.get(QueryConverter.QUERY_KEY))
                .setFetchSource((String[]) queryMap.get(QueryConverter.INCLUDES_KEY), (String[]) queryMap.get(QueryConverter.EXCLUDES_KEY));
        @SuppressWarnings("unchecked")
        Map<String, Object> sortFields = (Map<String, Object>) queryMap.get(QueryConverter.SORT_KEY);
        Iterator<String> mapIterator = sortFields.keySet().iterator();
        while (mapIterator.hasNext()) {
            String key = mapIterator.next();
            searchReqBuilder.addSort(key, SortOrder.valueOf((String) sortFields.get(key)));
        }
        SearchResponse response = searchReqBuilder
                .execute()
                .actionGet(TimeValue.timeValueMillis(getQueryTimeout()));
        SearchHit[] searchHits = response.getHits().getHits();
        if (searchHits == null) {
            return new ArrayList<T>();
        }
        List<T> resultList = new ArrayList<T>();
        int searchHitsSize = searchHits.length;

        for (SearchHit searchHit : searchHits) {
            resultList.add(modelContext.unmarshal(clazz, searchHit.sourceAsString()));
        }

        // // TODO verify total count
        // Long totalCount = null;
        // if (queryConverter.isAskTotalCount(query)) {
        // totalCount = response.getHits().getTotalHits();
        // }
        //
        // if (totalCount != null && totalCount > Integer.MAX_VALUE) {
        // throw new RuntimeException("Total hits exceeds integer max value");
        // }
        //
        // MessageListResultImpl result = new MessageListResultImpl(nextKey, totalCount);
        return resultList;
    }

    @Override
    public long count(TypeDescriptor typeDescriptor, Object query) throws ClientException {
        checkClient();
        // TODO check for fetch none
        Map<String, Object> queryMap = queryConverter.convertQuery(query);
        SearchRequestBuilder searchReqBuilder = client.prepareSearch(typeDescriptor.getIndex());
        SearchResponse response = searchReqBuilder.setTypes(typeDescriptor.getType())
                .setQuery(queryMap)
                .execute()
                .actionGet(TimeValue.timeValueMillis(getQueryTimeout()));
        SearchHits searchHits = response.getHits();

        if (searchHits == null)
            return 0;

        return searchHits.getTotalHits();
    }

    @Override
    public void delete(TypeDescriptor typeDescriptor, String id) throws ClientException {
        checkClient();
        client.prepareDelete()
                .setIndex(typeDescriptor.getIndex())
                .setType(typeDescriptor.getType())
                .setId(id)
                .get(TimeValue.timeValueMillis(getQueryTimeout()));
    }

    @Override
    public void deleteByQuery(TypeDescriptor typeDescriptor, Object query) throws ClientException {
        checkClient();
        Map<String, Object> queryMap = queryConverter.convertQuery(query);
        TimeValue queryTimeout = TimeValue.timeValueMillis(getQueryTimeout());
        TimeValue scrollTimeout = TimeValue.timeValueMillis(getScrollTimeout());

        // delete by query API is deprecated, scroll with bulk delete must be used
        SearchResponse scrollResponse = this.client.prepareSearch(typeDescriptor.getIndex())
                .setTypes(typeDescriptor.getType())
                .setFetchSource(false)
                .addSort("_doc", SortOrder.ASC)
                .setVersion(true)
                .setScroll(scrollTimeout)
                .setQuery(queryMap)
                .setSize(100)
                .get(queryTimeout);

        // Scroll until no hits are returned
        while (true) {

            // Break condition: No hits are returned
            if (scrollResponse.getHits().getHits().length == 0)
                break;

            BulkRequest bulkRequest = new BulkRequest();
            for (SearchHit hit : scrollResponse.getHits().hits()) {
                DeleteRequest delete = new DeleteRequest().index(hit.index())
                        .type(hit.type())
                        .id(hit.id())
                        .version(hit.version());
                bulkRequest.add(delete);
            }

            client.bulk(bulkRequest).actionGet(queryTimeout);

            scrollResponse = this.client.prepareSearchScroll(scrollResponse.getScrollId())
                    .setScroll(scrollTimeout)
                    .execute()
                    .actionGet(queryTimeout);
        }
    }

    @Override
    public IndexExistsResponse isIndexExists(IndexExistsRequest indexExistsRequest) throws ClientException {
        checkClient();
        IndicesExistsResponse response = client.admin().indices()
                .exists(new IndicesExistsRequest(indexExistsRequest.getIndex()))
                .actionGet();
        return new IndexExistsResponse(response.isExists());
    }

    @Override
    public void createIndex(String indexName, ObjectNode indexSettings) throws ClientException {
        checkClient();
        client.admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(indexSettings.toString())
                .execute()
                .actionGet();
    }

    @Override
    public boolean isMappingExists(TypeDescriptor typeDescriptor) throws ClientException {
        checkClient();
        GetMappingsRequest mappingsRequest = new GetMappingsRequest().indices(typeDescriptor.getIndex());
        GetMappingsResponse mappingsResponse = client.admin().indices().getMappings(mappingsRequest).actionGet();
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = mappingsResponse.getMappings();
        ImmutableOpenMap<String, MappingMetaData> map = mappings.get(typeDescriptor.getIndex());
        MappingMetaData metadata = map.get(typeDescriptor.getType());
        return metadata != null;
    }

    @Override
    public void putMapping(TypeDescriptor typeDescriptor, ObjectNode mapping) throws ClientException {
        checkClient();
        // Check message type mapping
        GetMappingsRequest mappingsRequest = new GetMappingsRequest().indices(typeDescriptor.getIndex());
        GetMappingsResponse mappingsResponse = client.admin().indices().getMappings(mappingsRequest).actionGet();
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = mappingsResponse.getMappings();
        ImmutableOpenMap<String, MappingMetaData> map = mappings.get(typeDescriptor.getIndex());
        MappingMetaData metadata = map.get(typeDescriptor.getType());
        if (metadata == null) {
            logger.info(mapping.toString());
            client.admin().indices().preparePutMapping(typeDescriptor.getIndex()).setType(typeDescriptor.getType()).setSource(mapping.toString()).execute().actionGet();
            logger.trace("Mapping {} created! ", typeDescriptor.getType());
        }
        else {
            logger.trace("Mapping {} already exists! ", typeDescriptor.getType());
        }
    }

    @Override
    public String getHashCode(String... components) {
        String concatString = "";
        for (String str : components) {
            concatString = concatString.concat(str);
        }
        byte[] hashCode = Hashing.sha256()
                .hashString(concatString, StandardCharsets.UTF_8)
                .asBytes();

        return Base64.encodeBytes(hashCode);
    }

    private void checkClient() throws ClientUndefinedException {
        if (client == null)
            throw new ClientUndefinedException(CLIENT_UNDEFINED_MSG);
    }

    /**
     * Get the query timeout (default value)
     * 
     * @return
     */
    public long getQueryTimeout() {
        // TODO move to configuration
        return 15000;
    }

    /**
     * Get the scroll timeout (default value)
     * 
     * @return
     */
    public static long getScrollTimeout() {
        return 60000;
    }

    // /**
    // * Build the upsert request
    // *
    // * @param metricInfoCreator
    // * @return
    // * @throws ClientDocumentBuilderException
    // */
    // public UpdateRequest getUpsertRequest(MetricInfoCreator metricInfoCreator)
    // throws ClientDocumentBuilderException {
    // String id = MetricInfoXContentBuilder.getOrDeriveId(null, metricInfoCreator);
    //
    // MetricInfoImpl metricInfo = new MetricInfoImpl(metricInfoCreator.getAccount(), new StorableIdImpl(id));
    // metricInfo.setClientId(metricInfoCreator.getClientId());
    // metricInfo.setChannel(metricInfoCreator.getChannel());
    // metricInfo.setFirstPublishedMessageId(metricInfoCreator.getMessageId());
    // metricInfo.setFirstPublishedMessageTimestamp(metricInfoCreator.getMessageTimestamp());
    // metricInfo.setName(metricInfoCreator.getName());
    // metricInfo.setType(metricInfoCreator.getType());
    // metricInfo.setValue(metricInfoCreator.getValue(Object.class));
    // return this.getUpsertRequest(metricInfo);
    // }
    //
    // /**
    // * Build the upsert request
    // *
    // * @param metricInfo
    // * @return
    // * @throws ClientDocumentBuilderException
    // */
    // public UpdateRequest getUpsertRequest(MetricInfo metricInfo)
    // throws ClientDocumentBuilderException {
    // MetricInfoXContentBuilder builder = new MetricInfoXContentBuilder();
    // builder.build(metricInfo);
    // List<MetricXContentBuilder> metricBuilders = builder.getBuilders();
    // return this.esTypeDAO.getUpsertRequest(metricBuilders.get(0).getId(), metricBuilders.get(0).getContent());
    // }
    //
    // /**
    // * Build the upsert request
    // *
    // * @param esChannelMetric
    // * @return
    // */
    // public UpdateRequest getUpsertRequest(MetricXContentBuilder esChannelMetric) {
    // return this.esTypeDAO.getUpsertRequest(esChannelMetric.getId(), esChannelMetric.getContent());
    // }
    //
    // /**
    // * Upsert action (insert the document (if not present) or update the document (if present) into the database)
    // *
    // * @param metricInfoCreator
    // * @return
    // * @throws ClientDocumentBuilderException
    // */
    // public UpdateResponse upsert(MetricInfoCreator metricInfoCreator)
    // throws ClientDocumentBuilderException {
    // String id = MetricInfoXContentBuilder.getOrDeriveId(null, metricInfoCreator);
    //
    // MetricInfoImpl metricInfo = new MetricInfoImpl(metricInfoCreator.getAccount(), new StorableIdImpl(id));
    // metricInfo.setChannel(metricInfoCreator.getChannel());
    // metricInfo.setFirstPublishedMessageId(metricInfoCreator.getMessageId());
    // metricInfo.setFirstPublishedMessageTimestamp(metricInfoCreator.getMessageTimestamp());
    // metricInfo.setName(metricInfoCreator.getName());
    // metricInfo.setType(metricInfoCreator.getType());
    // metricInfo.setValue(metricInfoCreator.getValue(Object.class));
    // return this.upsert(metricInfo);
    // }
    //
    // /**
    // * Upsert action (insert the document (if not present) or update the document (if present) into the database)
    // *
    // * @param metricInfo
    // * @return
    // * @throws ClientDocumentBuilderException
    // */
    // public UpdateResponse upsert(MetricInfo metricInfo)
    // throws ClientDocumentBuilderException {
    // MetricInfoXContentBuilder docBuilder = new MetricInfoXContentBuilder().build(metricInfo);
    // List<MetricXContentBuilder> metricInfos = docBuilder.getBuilders();
    // return esTypeDAO.upsert(metricInfos.get(0).getId(), metricInfos.get(0).getContent());
    // }

}
