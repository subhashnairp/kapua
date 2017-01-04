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
package org.eclipse.kapua.service.datastore.internal.elasticsearch;

import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Models a topic for messages posted to the Kapua platform.
 * Topic are expected to be in the form of "account/clientId/<application_specific>";
 * system topic starts with the $EDC account. 
 */
public class DatastoreChannel {
	
	@SuppressWarnings("unused")
	private static final Logger logger = LoggerFactory.getLogger(DatastoreChannel.class);
	
	public static final int MIN_PARTS = 3;
    public static final String MULTI_LEVEL_WCARD = "#";
    public static final String SINGLE_LEVEL_WCARD = "+";
    public static final String TOPIC_SEPARATOR = "/";

    public static final String ALERT_TOPIC = "ALERT";

    private String account;
    private String clientId;
	private String channel;
	private String[] channelParts;

	private void init(String account, String clientId, String channel) throws EsInvalidChannelException {
	    
		// Must be not null and not multilevel wild card
		if (account == null || MULTI_LEVEL_WCARD.equals(account))
			throw new EsInvalidChannelException("Invalid account: " + account);
		
		this.account = account;
		
		// Must be not null and not multilevel wild card
		if (clientId == null || MULTI_LEVEL_WCARD.equals(clientId))
			throw new EsInvalidChannelException("Invalid client id: " + clientId);
		
		this.clientId = clientId;
			
		// Must be not null and not singlelevel wild card
        if (channel == null || SINGLE_LEVEL_WCARD.equals(channel))
			throw new EsInvalidChannelException("Invalid channel: " + channel);
		
		this.channel = channel;		
		this.channelParts = this.channel.split(TOPIC_SEPARATOR);
		
		if (this.channelParts.length < 1) {
			// Special case: The topic is too small
			throw new EsInvalidChannelException(channel);
		}
	}
	
    public DatastoreChannel(String account, String clientId, List<String> channelParts) throws EsInvalidChannelException {		
		this(account, clientId, (new Object() {
				@Override
				public String toString() {
					return getChannel(channelParts);						
				}
		}).toString());
    }
    
	public DatastoreChannel(String account, String clientId, String channel) throws EsInvalidChannelException {
		init(account, clientId, channel);
	}
	
	public DatastoreChannel(String fullName) throws EsInvalidChannelException {
	    
		// Must be not null and not multilevel wild card
		if (fullName == null)
			throw new EsInvalidChannelException("Invalid channel: " + fullName);
		
		String[] parts = fullName.split(Pattern.quote(TOPIC_SEPARATOR));
		if (parts.length < MIN_PARTS)
			throw new EsInvalidChannelException(String.format("Invalid channel: less than %d parts found.", MIN_PARTS));
		
		init(account, clientId, channel);
	}
	
	public String getAccount()
	{
		return this.account;
	}
	
	public boolean isAnyAccount()
	{
		return SINGLE_LEVEL_WCARD.equals(this.account);
	}
	
	public String getClientId()
	{
		return this.clientId;
	}
	
	public static boolean isAnyClientId(String clientId)
	{
		return SINGLE_LEVEL_WCARD.equals(clientId);
	}
	
	public boolean isAnyClientId()
	{
		return isAnyClientId(clientId);
	}
	
	public static boolean isAlertTopic(String channel) 
	{
		return ALERT_TOPIC.equals(channel);
	}
	
	public boolean isAlertTopic() {
		return isAlertTopic(this.channel);
	}
    
    public static boolean isAnySubtopic(String channel) 
    {
    	if (channel == null)
    		return false;
    	
        final String multilevelAnySubtopic = String.format("%s%s", TOPIC_SEPARATOR, MULTI_LEVEL_WCARD);
        boolean isAnySubtopic = channel.endsWith(multilevelAnySubtopic) ||
                                MULTI_LEVEL_WCARD.equals(channel);
        
        return isAnySubtopic;
    }	
    
    public boolean isAnySubtopic() 
    {
        return isAnySubtopic(this.channel);
    }	

    public static String getChannel(List<String> parts)
    {
		StringBuilder channelBuilder = new StringBuilder();
		for (String part:parts) {
			channelBuilder.append(part);
			channelBuilder.append(DatastoreChannel.TOPIC_SEPARATOR);
		}
		return 	channelBuilder.toString();						
    }
    
	public String getChannel() {
		return channel;
	}

	public String getLeafName() {
		return this.channelParts[this.channelParts.length-1];
	}

	public String getParentTopic() {
		int iLastSlash = channel.lastIndexOf(TOPIC_SEPARATOR); 
		return iLastSlash != -1 ? channel.substring(0, iLastSlash) : null;
	}

	public String getGrandParentTopic() {
		String parentTopic = getParentTopic();
		if (parentTopic != null) {
			int iLastSlash = parentTopic.lastIndexOf(TOPIC_SEPARATOR); 
			return iLastSlash != -1 ? parentTopic.substring(0, iLastSlash) : null;
		}
		else {
			return null;
		}
	}
	
	public String[] getParts() {
		return this.channelParts;
	}

	public String getFullName() {
		return this.account + TOPIC_SEPARATOR + this.clientId + TOPIC_SEPARATOR + this.channel;
	}
	
	@Override
	public String toString() {
		return this.channel;
	}
	}
