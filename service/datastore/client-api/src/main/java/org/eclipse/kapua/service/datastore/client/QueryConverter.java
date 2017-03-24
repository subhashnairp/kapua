package org.eclipse.kapua.service.datastore.client;

import java.util.Map;

public interface QueryConverter {

    public final static String QUERY_KEY = "query";
    public final static String SORT_KEY = "sort";
    public final static String SOURCE_KEY = "_source";
    public final static String INCLUDES_KEY = "include";
    public final static String EXCLUDES_KEY = "exclude";

    public final static String SORT_ASCENDING_VALUE = "asc";
    public final static String SORT_DESCENDING_VALUE = "desc";

    public Map<String, Object> convertQuery(Object query) throws QueryMappingException;

}
