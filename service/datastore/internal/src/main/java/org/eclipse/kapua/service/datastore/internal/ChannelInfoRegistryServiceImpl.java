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
package org.eclipse.kapua.service.datastore.internal;

import org.eclipse.kapua.KapuaException;
import org.eclipse.kapua.commons.configuration.AbstractKapuaConfigurableService;
import org.eclipse.kapua.commons.util.ArgumentValidator;
import org.eclipse.kapua.locator.KapuaLocator;
import org.eclipse.kapua.locator.KapuaProvider;
import org.eclipse.kapua.model.id.KapuaId;
import org.eclipse.kapua.service.account.AccountService;
import org.eclipse.kapua.service.authorization.AuthorizationService;
import org.eclipse.kapua.service.authorization.domain.Domain;
import org.eclipse.kapua.service.authorization.permission.Actions;
import org.eclipse.kapua.service.authorization.permission.Permission;
import org.eclipse.kapua.service.authorization.permission.PermissionFactory;
import org.eclipse.kapua.service.datastore.MessageStoreService;
import org.eclipse.kapua.service.datastore.ChannelInfoRegistryService;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.DatastoreMediator;
import org.eclipse.kapua.service.datastore.model.StorableId;
import org.eclipse.kapua.service.datastore.model.ChannelInfo;
import org.eclipse.kapua.service.datastore.model.ChannelInfoListResult;
import org.eclipse.kapua.service.datastore.model.query.ChannelInfoQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@KapuaProvider
public class ChannelInfoRegistryServiceImpl extends AbstractKapuaConfigurableService implements ChannelInfoRegistryService
{
    private static final long   serialVersionUID = 7839070776817998600L;

    private static final Domain datastoreDomain = new DatastoreDomain();
    
    @SuppressWarnings("unused")
    private static final Logger logger           = LoggerFactory.getLogger(ChannelInfoRegistryServiceImpl.class);

    private final AccountService      accountService;
    private final AuthorizationService        authorizationService;
    private final PermissionFactory           permissionFactory;
    private final ChannelInfoRegistryFacade channelInfoStoreFacade;

    public ChannelInfoRegistryServiceImpl()
    {
        super(ChannelInfoRegistryService.class.getName(), datastoreDomain, DatastoreEntityManagerFactory.getInstance());

        KapuaLocator locator = KapuaLocator.getInstance();
        accountService = locator.getService(AccountService.class);
        authorizationService = locator.getService(AuthorizationService.class);
        permissionFactory = locator.getFactory(PermissionFactory.class);
        
        MessageStoreService messageStoreService = KapuaLocator.getInstance().getService(MessageStoreService.class);
        ConfigurationProviderImpl configurationProvider = new ConfigurationProviderImpl(messageStoreService, accountService);
        this.channelInfoStoreFacade = new ChannelInfoRegistryFacade(configurationProvider, DatastoreMediator.getInstance());
        DatastoreMediator.getInstance().setChannelInfoStoreFacade(channelInfoStoreFacade);
    }

    @Override
    public void delete(KapuaId scopeId, StorableId id)
        throws KapuaException
    {
    	try 
    	{
	        ArgumentValidator.notNull(scopeId, "scopeId");
	
	        this.checkDataAccess(scopeId, Actions.delete);
	
	        this.channelInfoStoreFacade.delete(scopeId, id);
    	} 
    	catch (Exception e) {
    		throw KapuaException.internalError(e);
    	}
    }

    @Override
    public ChannelInfo find(KapuaId scopeId, StorableId id)
        throws KapuaException
    {
    	try 
    	{
	        ArgumentValidator.notNull(scopeId, "scopeId");
	
	        this.checkDataAccess(scopeId, Actions.read);
	        
	        return this.channelInfoStoreFacade.find(scopeId, id);
    	}
    	catch (Exception e) {
    		throw KapuaException.internalError(e);
    	}
    }

    @Override
    public ChannelInfoListResult query(KapuaId scopeId, ChannelInfoQuery query)
        throws KapuaException
    {
    	try 
    	{
	        ArgumentValidator.notNull(scopeId, "scopeId");
	
	        this.checkDataAccess(scopeId, Actions.read);
	        
	        return this.channelInfoStoreFacade.query(scopeId, query);
    	}
    	catch (Exception e) {
    		throw KapuaException.internalError(e);
    	}
   }

    @Override
    public long count(KapuaId scopeId, ChannelInfoQuery query)
        throws KapuaException
    {
    	try 
    	{
	        ArgumentValidator.notNull(scopeId, "scopeId");
	
	        this.checkDataAccess(scopeId, Actions.read);
	
	        return this.channelInfoStoreFacade.count(scopeId, query);
    	}
    	catch (Exception e) {
    		throw KapuaException.internalError(e);
    	}
    }

    @Override
    public void delete(KapuaId scopeId, ChannelInfoQuery query)
        throws KapuaException
    {
    	try
    	{
	        ArgumentValidator.notNull(scopeId, "scopeId");
	        
	        this.checkDataAccess(scopeId, Actions.delete);
	
	        this.delete(scopeId, query);
    	}
    	catch (Exception e) {
    		throw KapuaException.internalError(e);
    	}
    }

    private void checkDataAccess(KapuaId scopeId, Actions action)
        throws KapuaException
    {
        //
        // Check Access
        Permission permission = permissionFactory.newPermission(datastoreDomain, action, scopeId);
        authorizationService.checkPermission(permission);
    }
}
