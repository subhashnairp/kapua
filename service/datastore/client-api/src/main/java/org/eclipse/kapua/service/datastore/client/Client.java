package org.eclipse.kapua.service.datastore.client;

import java.util.List;

import com.fasterxml.jackson.databind.node.ObjectNode;

public interface Client {

    /**
     * Insert
     * 
     * @param insertRequest
     * @return
     * @throws ClientException
     */
    public InsertResponse insert(InsertRequest insertRequest) throws ClientException;

    /**
     * Upsert
     * 
     * @param updateRequest
     * @return
     * @throws ClientException
     */
    public UpdateResponse upsert(UpdateRequest updateRequest) throws ClientException;

    /**
     * Bulk upsert
     * 
     * @param bulkUpdateRequest
     * @return
     * @throws ClientException
     */
    public BulkUpdateResponse upsert(BulkUpdateRequest bulkUpdateRequest) throws ClientException;

    /**
     * Find by query (this method returns the first result found that matches the quesy)
     * 
     * @param typeDescriptor
     * @param query
     * @param clazz
     * @return
     * @throws ClientException
     */
    public <T> T find(TypeDescriptor typeDescriptor, Object query, Class<T> clazz) throws ClientException;

    /**
     * Find by query criteria
     * 
     * @param typeDescriptor
     * @param query
     * @param clazz
     * @return
     * @throws ClientException
     */
    public <T> List<T> query(TypeDescriptor typeDescriptor, Object query, Class<T> clazz) throws ClientException;

    /**
     * Count by query criteria
     * 
     * @param typeDescriptor
     * @param query
     * @return
     * @throws ClientException
     */
    public long count(TypeDescriptor typeDescriptor, Object query) throws ClientException;

    /**
     * Delete by id
     * 
     * @param typeDescriptor
     * @param id
     * @throws ClientException
     */
    public void delete(TypeDescriptor typeDescriptor, String id) throws ClientException;

    /**
     * Delete by query criteria
     * 
     * @param typeDescriptor
     * @param query
     * @throws ClientException
     */
    public void deleteByQuery(TypeDescriptor typeDescriptor, Object query) throws ClientException;

    // Indexes / mappings section

    /**
     * Check if the index exists
     * 
     * @param indexExistsRequest
     * @return
     * @throws ClientException
     */
    public IndexExistsResponse isIndexExists(IndexExistsRequest indexExistsRequest) throws ClientException;

    /**
     * Create the index
     * 
     * @param indexName
     * @param indexDescriptor
     * @throws ClientException
     */
    public void createIndex(String indexName, ObjectNode indexSettings) throws ClientException;

    /**
     * Check if the mapping exists
     * 
     * @param typeDescriptor
     * @return
     * @throws ClientException
     */
    public boolean isMappingExists(TypeDescriptor typeDescriptor) throws ClientException;

    /**
     * Put the mapping
     * 
     * @param typeDescriptor
     * @param mapping
     * @throws ClientException
     */
    public void putMapping(TypeDescriptor typeDescriptor, ObjectNode mapping) throws ClientException;

    // utilities

    /**
     * Concatenate components and return the hash code (base64)
     * 
     * @param components
     * @return
     */
    public String getHashCode(String... components);

    // ModelContext

    /**
     * Set the model context
     * 
     * @param modelContext
     */
    public void setModelContext(ModelContext modelContext);

    public void setQueryConverter(QueryConverter queryConverter);
}
