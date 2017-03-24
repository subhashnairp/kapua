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
package org.eclipse.kapua.service.datastore.client.transport;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import org.eclipse.kapua.service.datastore.client.ClientUnavailableException;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

/**
 * Elasticsearch transport client implementation.
 *
 * @since 1.0
 *
 */
public class TransportClientProvider implements ElasticsearchClientProvider {

    private static final int DEFAULT_PORT = 9300;

    private Client client;

    private static String[] getNodeParts(String node) {
        if (node == null)
            return new String[] {};

        String[] split = node.split(":");
        return split;
    }

    private static Client getEsClient(String hostname, int port, String clustername) throws UnknownHostException {

        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", clustername).build();

        InetSocketTransportAddress ita = new InetSocketTransportAddress(InetAddress.getByName(hostname), port);
        Client esClient = TransportClient.builder().settings(settings).build().addTransportAddress(ita);

        return esClient;
    }

    /**
     * Create a new Elasticsearch transport client based on the configuration parameters ({@link ClientSettingKey})
     *
     * @throws EsClientUnavailableException
     */
    public TransportClientProvider()
            throws ClientUnavailableException {
        ClientSettings config = ClientSettings.getInstance();
        Map<String, String> map = config.getMap(String.class, ClientSettingKey.ELASTICSEARCH_NODES, "[0-9]+");
        String[] esNodes = new String[] {};
        if (map != null)
            esNodes = map.values().toArray(new String[] {});

        if (esNodes == null || esNodes.length == 0)
            throw new ClientUnavailableException("No elasticsearch nodes found");

        String[] nodeParts = getNodeParts(esNodes[0]);
        String esHost = null;
        int esPort = DEFAULT_PORT;

        if (nodeParts.length > 0)
            esHost = nodeParts[0];

        if (nodeParts.length > 1) {
            try {
                Integer.parseInt(nodeParts[1]);
            } catch (NumberFormatException e) {
                throw new ClientUnavailableException("Could not parse elasticsearch port: " + nodeParts[1]);
            }
        }

        Client theClient = null;
        try {
            theClient = getEsClient(esHost, esPort, config.getString(ClientSettingKey.ELASTICSEARCH_CLUSTER));
        } catch (UnknownHostException e) {
            throw new ClientUnavailableException("Unknown elasticsearch node host", e);
        }

        client = theClient;

    }

    @Override
    public Client getClient() {
        return client;
    }

}
