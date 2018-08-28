package org.nuxeo.labs.nifi;

import org.apache.nifi.controller.ControllerService;
import org.nuxeo.client.NuxeoClient;

/**
 * {@link NuxeoClient} Service Factory.
 * 
 * Obtain a valid {@link NuxeoClient} instance to manipulate documents within the repository.
 */
public interface NuxeoClientService extends ControllerService {

    /**
     * Retrieve the configured {@link NuxeoClient} instance
     * 
     * @return the {@link NuxeoClient}
     */
    NuxeoClient getClient();

    /**
     * Retrieve the configured default Nuxeo repository, if configured
     * 
     * @return the default Nuxeo repository
     */
    String getDefaultRepository();

}
