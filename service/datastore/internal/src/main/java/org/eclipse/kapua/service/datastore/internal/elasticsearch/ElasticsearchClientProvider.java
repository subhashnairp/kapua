package org.eclipse.kapua.service.datastore.internal.elasticsearch;

import org.elasticsearch.client.Client;

public interface ElasticsearchClientProvider {

	public Client getClient();
}
