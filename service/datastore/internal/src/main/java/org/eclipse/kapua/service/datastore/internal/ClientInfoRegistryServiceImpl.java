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
import org.eclipse.kapua.service.datastore.ClientInfoRegistryService;
import org.eclipse.kapua.service.datastore.MessageStoreService;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.DatastoreMediator;
import org.eclipse.kapua.service.datastore.model.ClientInfo;
import org.eclipse.kapua.service.datastore.model.ClientInfoListResult;
import org.eclipse.kapua.service.datastore.model.StorableId;
import org.eclipse.kapua.service.datastore.model.query.ClientInfoQuery;
import org.eclipse.kapua.service.device.registry.DeviceRegistryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@KapuaProvider
public class ClientInfoRegistryServiceImpl extends AbstractKapuaConfigurableService implements ClientInfoRegistryService
{
	private static final long serialVersionUID = 6772144495298409738L;
	
	private static final Domain datastoreDomain = new DatastoreDomain();

	@SuppressWarnings("unused")
	private static final Logger logger = LoggerFactory.getLogger(ClientInfoRegistryServiceImpl.class);

	private final AccountService accountService;
	private final DeviceRegistryService deviceRegistryService;
	private final AuthorizationService authorizationService;
	private final PermissionFactory permissionFactory;
	private final ClientInfoRegistryFacade facade;

    public ClientInfoRegistryServiceImpl()
    {
        super(ClientInfoRegistryService.class.getName(), datastoreDomain, DatastoreEntityManagerFactory.getInstance());

        KapuaLocator locator = KapuaLocator.getInstance();
        accountService = locator.getService(AccountService.class);
        deviceRegistryService = locator.getService(DeviceRegistryService.class);
        authorizationService = locator.getService(AuthorizationService.class);
        permissionFactory = locator.getFactory(PermissionFactory.class);
        
        MessageStoreService messageStoreService = KapuaLocator.getInstance().getService(MessageStoreService.class);
        ConfigurationProviderImpl configurationProvider = new ConfigurationProviderImpl(messageStoreService, accountService, deviceRegistryService);
        this.facade = new ClientInfoRegistryFacade(configurationProvider, DatastoreMediator.getInstance());
        DatastoreMediator.getInstance().setClientInfoStoreFacade(this.facade);
    }

    @Override
    public void delete(KapuaId scopeId, StorableId id) 
    		throws KapuaException
    {
    	try  
    	{
	        ArgumentValidator.notNull(scopeId, "scopeId");
	        
	        this.checkAccess(scopeId, Actions.delete);
	
	        this.facade.delete(scopeId, id);
    	} 
    	catch (Exception e) {
    		throw KapuaException.internalError(e);
    	}
     }

    @Override
    public ClientInfo find(KapuaId scopeId, StorableId id)
        throws KapuaException
    {
    	try 
    	{
	        ArgumentValidator.notNull(scopeId, "scopeId");
	        
	        this.checkAccess(scopeId, Actions.read);
	
	        return this.facade.find(scopeId, id);
    	} 
    	catch (Exception e) {
    		throw KapuaException.internalError(e);
    	}
    }

    @Override
    public ClientInfoListResult query(KapuaId scopeId, ClientInfoQuery query)
        throws KapuaException
    {
    	try 
    	{
	        ArgumentValidator.notNull(scopeId, "scopeId");
	        
	        this.checkAccess(scopeId, Actions.read);
	        
	        return this.facade.query(scopeId, query);
    	} 
    	catch (Exception e) {
    		throw KapuaException.internalError(e);
    	}
    }

    @Override
    public long count(KapuaId scopeId, ClientInfoQuery query)
        throws KapuaException
    {
    	try 
    	{
	        //
	        // Argument Validation
	        ArgumentValidator.notNull(scopeId, "scopeId");
	        
	        //
	        // Check Access
	        this.checkAccess(scopeId, Actions.read);
	
	        return this.facade.count(scopeId, query);
    	} 
    	catch (Exception e) {
    		throw KapuaException.internalError(e);
    	}
    }

    @Override
    public void delete(KapuaId scopeId, ClientInfoQuery query)
        throws KapuaException
    {
    	try 
    	{
	        ArgumentValidator.notNull(scopeId, "scopeId");
	        
	        this.checkAccess(scopeId, Actions.delete);
	
	        this.facade.delete(scopeId, query);
    	} 
    	catch (Exception e) {
    		throw KapuaException.internalError(e);
    	}
    }

    private void checkAccess(KapuaId scopeId, Actions action)
        throws KapuaException
    {
        //
        // Check Access
        Permission permission = permissionFactory.newPermission(datastoreDomain, action, scopeId);
        authorizationService.checkPermission(permission);
    }
}
