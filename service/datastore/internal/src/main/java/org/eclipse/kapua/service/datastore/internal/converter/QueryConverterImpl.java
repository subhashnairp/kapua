package org.eclipse.kapua.service.datastore.internal.converter;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.kapua.service.datastore.client.QueryConverter;
import org.eclipse.kapua.service.datastore.client.QueryMappingException;
import org.eclipse.kapua.service.datastore.internal.AbstractStorableQuery;

public class QueryConverterImpl implements QueryConverter {

    @Override
    /**
     * <pre>
     * "_source": {
     *      "include": [ "obj1.*", "obj2.*" ],
     *      "exclude": [ "*.description" ]
     *  },
     *  "sort" : [
     *      { "post_date" : {"order" : "asc"}},
     *      "user",
     *      { "name" : "desc" },
     *      { "age" : "desc" },
     *      "_score"
     *  ],
     *  "query" : {
     *      "term" : { "user" : "kimchy" }
     *  }
     * </pre>
     */
    public Map<String, Object> convertQuery(Object query) throws QueryMappingException {
        if (query instanceof AbstractStorableQuery<?>) {
            Map<String, Object> mapResult = new HashMap<>();
            AbstractStorableQuery<?> storableQuery = (AbstractStorableQuery<?>) query;
            // includes/excludes
            Map<String, Object> includesMap = new HashMap<>();
            includesMap.put(INCLUDES_KEY, storableQuery.getIncludes(storableQuery.getFetchStyle()));
            includesMap.put(EXCLUDES_KEY, storableQuery.getExcludes(storableQuery.getFetchStyle()));
            mapResult.put(SOURCE_KEY, includesMap);
            // query
            mapResult.put(QUERY_KEY, storableQuery.getPredicate().toSerializedMap());
            // sort
            Map<String, Object> sortMap = new HashMap<>();

            mapResult.put(SORT_KEY, sortMap);
            return mapResult;
        } else {
            throw new QueryMappingException("Wrong query type! Only AbstractStorableQuery can be converted!");
        }
    }

}
