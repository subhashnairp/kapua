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
package org.eclipse.kapua.app.api.v1.resources;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.eclipse.kapua.app.api.v1.resources.model.CountResult;
import org.eclipse.kapua.app.api.v1.resources.model.ScopeId;
import org.eclipse.kapua.app.api.v1.resources.model.StorableEntityId;
import org.eclipse.kapua.locator.KapuaLocator;
import org.eclipse.kapua.service.datastore.ChannelInfoRegistryService;
import org.eclipse.kapua.service.datastore.DatastoreObjectFactory;
import org.eclipse.kapua.service.datastore.model.ChannelInfo;
import org.eclipse.kapua.service.datastore.model.ChannelInfoListResult;
import org.eclipse.kapua.service.datastore.model.query.ChannelInfoQuery;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Api("Data Channels")
@Path("{scopeId}/data/channels")
public class DataChannels extends AbstractKapuaResource {

    private final KapuaLocator locator = KapuaLocator.getInstance();
    private final ChannelInfoRegistryService channelInfoRegistryService = locator.getService(ChannelInfoRegistryService.class);
    private final DatastoreObjectFactory datastoreObjectFactory = locator.getFactory(DatastoreObjectFactory.class);

    /**
     * Gets the {@link ChannelInfo} list in the scope.
     *
     * @param scopeId
     *            The {@link ScopeId} in which to search results.
     * @param offset
     *            The result set offset.
     * @param limit
     *            The result set limit.
     * @return The {@link ChannelInfoListResult} of all the channelInfos associated to the current selected scope.
     * @since 1.0.0
     */
    @ApiOperation(value = "Gets the ChannelInfo list in the scope", //
            notes = "Returns the list of all the channelInfos associated to the current selected scope.", //
            response = ChannelInfo.class, //
            responseContainer = "ChannelInfoListResult")
    @GET
    @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
    public ChannelInfoListResult simpleQuery( //
            @ApiParam(value = "The ScopeId in which to search results", required = true, defaultValue = DEFAULT_SCOPE_ID) @PathParam("scopeId") ScopeId scopeId,//
            @ApiParam(value = "The result set offset", defaultValue = "0") @QueryParam("offset") @DefaultValue("0") int offset,//
            @ApiParam(value = "The result set limit", defaultValue = "50") @QueryParam("limit") @DefaultValue("50") int limit) //
    {
        ChannelInfoListResult channelInfoListResult = datastoreObjectFactory.newChannelInfoListResult();
        try {
            ChannelInfoQuery query = datastoreObjectFactory.newChannelInfoQuery(scopeId);

            query.setOffset(offset);
            query.setLimit(limit);

            channelInfoListResult = query(scopeId, query);
        } catch (Throwable t) {
            handleException(t);
        }
        return channelInfoListResult;
    }

    /**
     * Queries the results with the given {@link ChannelInfoQuery} parameter.
     * 
     * @param scopeId
     *            The {@link ScopeId} in which to search results.
     * @param query
     *            The {@link ChannelInfoQuery} to used to filter results.
     * @return The {@link ChannelInfoListResult} of all the result matching the given {@link ChannelInfoQuery} parameter.
     * @since 1.0.0
     */
    @POST
    @Path("_query")
    @Consumes({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @ApiOperation(value = "Queries the ChannelInfos", //
            notes = "Queries the ChannelInfos with the given ChannelInfoQuery parameter returning all matching ChannelInfos",  //
            response = ChannelInfo.class, //
            responseContainer = "ChannelInfoListResult")  //
    public ChannelInfoListResult query( //
            @ApiParam(value = "The ScopeId in which to search results", required = true, defaultValue = DEFAULT_SCOPE_ID) @PathParam("scopeId") ScopeId scopeId, //
            @ApiParam(value = "The ChannelInfoQuery to use to filter results", required = true) ChannelInfoQuery query) {
        ChannelInfoListResult channelInfoListResult = null;
        try {
            query.setScopeId(scopeId);
            channelInfoListResult = channelInfoRegistryService.query(query);
        } catch (Throwable t) {
            handleException(t);
        }
        return returnNotNullEntity(channelInfoListResult);
    }

    /**
     * Counts the results with the given {@link ChannelInfoQuery} parameter.
     * 
     * @param scopeId
     *            The {@link ScopeId} in which to search results.
     * @param query
     *            The {@link ChannelInfoQuery} to used to filter results.
     * @return The count of all the result matching the given {@link ChannelInfoQuery} parameter.
     * @since 1.0.0
     */
    @POST
    @Path("_count")
    @Consumes({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @ApiOperation(value = "Counts the ChannelInfos", //
            notes = "Counts the ChannelInfos with the given ChannelInfoQuery parameter returning the number of matching ChannelInfos", //
            response = CountResult.class)
    public CountResult count( //
            @ApiParam(value = "The ScopeId in which to search results", required = true, defaultValue = DEFAULT_SCOPE_ID) @PathParam("scopeId") ScopeId scopeId, //
            @ApiParam(value = "The ChannelInfoQuery to use to filter count results", required = true) ChannelInfoQuery query) {
        CountResult countResult = null;
        try {
            query.setScopeId(scopeId);
            countResult = new CountResult(channelInfoRegistryService.count(query));
        } catch (Throwable t) {
            handleException(t);
        }
        return returnNotNullEntity(countResult);
    }

    /**
     * Returns the ChannelInfo specified by the "channelInfoId" path parameter.
     *
     * @param channelInfoId
     *            The id of the requested ChannelInfo.
     * @return The requested ChannelInfo object.
     */
    @GET
    @Path("{channelInfoId}")
    @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
    @ApiOperation(value = "Gets an ChannelInfo", //
            notes = "Gets the ChannelInfo specified by the channelInfoId path parameter", //
            response = ChannelInfo.class)
    public ChannelInfo find( //
            @ApiParam(value = "The ScopeId of the requested ChannelInfo.", required = true, defaultValue = DEFAULT_SCOPE_ID) @PathParam("scopeId") ScopeId scopeId,
            @ApiParam(value = "The id of the requested ChannelInfo", required = true) @PathParam("channelInfoId") StorableEntityId channelInfoId) {
        ChannelInfo channelInfo = null;
        try {
            channelInfo = channelInfoRegistryService.find(scopeId, channelInfoId);
        } catch (Throwable t) {
            handleException(t);
        }
        return returnNotNullEntity(channelInfo);
    }
}
