package org.nuxeo.labs.nifi;

import org.apache.nifi.controller.ControllerService;
import org.nuxeo.client.NuxeoClient;

public interface NuxeoClientService extends ControllerService {
	
	NuxeoClient getClient();
	
	String getDefaultRepository();
	
}
