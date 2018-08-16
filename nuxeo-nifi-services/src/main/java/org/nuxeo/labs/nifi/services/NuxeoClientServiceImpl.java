package org.nuxeo.labs.nifi.services;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.nuxeo.client.NuxeoClient;
import org.nuxeo.labs.nifi.NuxeoClientService;

@Tags({ "nuxeo", "configuration" })
@CapabilityDescription("Provides a controller service to manage Nuxeo server connections.")
public class NuxeoClientServiceImpl extends AbstractControllerService implements NuxeoClientService {

	public static final PropertyDescriptor SERVER_URL = new PropertyDescriptor.Builder().name("SERVER_URL")
			.displayName("Server URL").description("Nuxeo Server URL.").defaultValue("http://localhost:8080/nuxeo")
			.required(true).addValidator(StandardValidators.URL_VALIDATOR).build();

	public static final PropertyDescriptor DEFAULT_REPO = new PropertyDescriptor.Builder().name("DEFAULT_REPO")
			.displayName("Default Repository").description("Default repository to use.").required(false)
			.addValidator(Validator.VALID).build();

	public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder().name("USERNAME")
			.displayName("Username").description("Nuxeo User.").required(true)
			.addValidator(StandardValidators.NON_BLANK_VALIDATOR).build();

	public static final PropertyDescriptor CREDENTIALS = new PropertyDescriptor.Builder().name("CREDENTIALS")
			.displayName("Credentials").description("Nuxeo User Credentials.").sensitive(true).required(true)
			.addValidator(StandardValidators.NON_BLANK_VALIDATOR).build();

	private static final List<PropertyDescriptor> serviceProperties;

	static {
		final List<PropertyDescriptor> props = new ArrayList<>();
		props.add(SERVER_URL);
		props.add(DEFAULT_REPO);
		props.add(USERNAME);
		props.add(CREDENTIALS);
		serviceProperties = Collections.unmodifiableList(props);
	}

	private String serverUrl;

	private String defaultRepo;

	private String username;

	private String credentials;

	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return serviceProperties;
	}

	@OnEnabled
	public void onConfigured(final ConfigurationContext context) throws InitializationException {
		// Destination server
		serverUrl = context.getProperty(SERVER_URL).getValue();

		// Default repo
		defaultRepo = context.getProperty(DEFAULT_REPO).getValue();

		// Credentials
		username = context.getProperty(USERNAME).getValue();
		credentials = context.getProperty(CREDENTIALS).getValue();
	}

	public NuxeoClient getClient() {
		// Build it
		NuxeoClient client = new NuxeoClient.Builder()
				// Set URL
				.url(serverUrl)
				// Authenticate
				.authentication(username, credentials)
				// Connect
				.connect();
		// Ship it
		return client;
	}

	public String getDefaultRepository() {
		return this.defaultRepo;
	}

}
