package org.nuxeo.labs.nifi.services;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.nuxeo.client.NuxeoClient;
import org.nuxeo.client.spi.auth.BasicAuthInterceptor;
import org.nuxeo.client.spi.auth.PortalSSOAuthInterceptor;
import org.nuxeo.client.spi.auth.TokenAuthInterceptor;
import org.nuxeo.labs.nifi.NuxeoClientService;

import okhttp3.Interceptor;

@Tags({ "nuxeo", "configuration" })
@CapabilityDescription("Provides a controller service to manage Nuxeo server connections.")
public class NuxeoClientServiceImpl extends AbstractControllerService implements NuxeoClientService {

    static final String BASIC = "Basic";

    static final String TOKEN = "Token";

    static final String PORTAL = "Portal";

    public static final PropertyDescriptor SERVER_URL = new PropertyDescriptor.Builder().name("SERVER_URL")
                                                                                        .displayName("Server URL")
                                                                                        .description(
                                                                                                "Nuxeo Server URL.")
                                                                                        .defaultValue(
                                                                                                "http://localhost:8080/nuxeo")
                                                                                        .expressionLanguageSupported(
                                                                                                ExpressionLanguageScope.VARIABLE_REGISTRY)
                                                                                        .required(true)
                                                                                        .addValidator(
                                                                                                StandardValidators.URL_VALIDATOR)
                                                                                        .build();

    public static final PropertyDescriptor DEFAULT_REPO = new PropertyDescriptor.Builder().name("DEFAULT_REPO")
                                                                                          .displayName(
                                                                                                  "Default Repository")
                                                                                          .description(
                                                                                                  "Default repository to use.")
                                                                                          .expressionLanguageSupported(
                                                                                                  ExpressionLanguageScope.VARIABLE_REGISTRY)
                                                                                          .required(false)
                                                                                          .addValidator(Validator.VALID)
                                                                                          .build();

    public static final PropertyDescriptor AUTH_TYPE = new PropertyDescriptor.Builder().name("AUTH_TYPE")
                                                                                       .displayName(
                                                                                               "Authentication Type")
                                                                                       .description(
                                                                                               "The type of authentication to use with the Nuxeo Server.")
                                                                                       .required(true)
                                                                                       .allowableValues(
                                                                                               new AllowableValue(BASIC,
                                                                                                       BASIC,
                                                                                                       "Username and Password credentials authentication."),
                                                                                               new AllowableValue(TOKEN,
                                                                                                       TOKEN,
                                                                                                       "Token credential authentication. Username is required but ignored."),
                                                                                               new AllowableValue(
                                                                                                       PORTAL, PORTAL,
                                                                                                       "Username and Secret credentials authentication."))
                                                                                       .defaultValue(BASIC)
                                                                                       .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder().name("USERNAME")
                                                                                      .displayName("Username")
                                                                                      .description("Nuxeo User.")
                                                                                      .expressionLanguageSupported(
                                                                                              ExpressionLanguageScope.VARIABLE_REGISTRY)
                                                                                      .required(true)
                                                                                      .addValidator(
                                                                                              StandardValidators.NON_BLANK_VALIDATOR)
                                                                                      .build();

    public static final PropertyDescriptor CREDENTIALS = new PropertyDescriptor.Builder().name("CREDENTIALS")
                                                                                         .displayName("Credentials")
                                                                                         .description(
                                                                                                 "Nuxeo User Credentials.")
                                                                                         .expressionLanguageSupported(
                                                                                                 ExpressionLanguageScope.VARIABLE_REGISTRY)
                                                                                         .sensitive(true)
                                                                                         .required(true)
                                                                                         .addValidator(
                                                                                                 StandardValidators.NON_BLANK_VALIDATOR)
                                                                                         .build();

    private static final List<PropertyDescriptor> serviceProperties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(SERVER_URL);
        props.add(DEFAULT_REPO);
        props.add(AUTH_TYPE);
        props.add(USERNAME);
        props.add(CREDENTIALS);
        serviceProperties = Collections.unmodifiableList(props);
    }

    private String serverUrl;

    private String defaultRepo;

    private String authType;

    private String username;

    private String credentials;

    private NuxeoClient client;

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
        authType = context.getProperty(AUTH_TYPE).getValue();
        username = context.getProperty(USERNAME).getValue();
        credentials = context.getProperty(CREDENTIALS).getValue();

        if (!serverUrl.endsWith("/nuxeo") && !serverUrl.endsWith("/nuxeo/")) {
            getLogger().warn("Server URL does not end with '/nuxeo': " + serverUrl);
        }

        try {
            // Test client build
            this.client = buildClient();
        } catch (Exception ex) {
            throw new InitializationException(ex);
        }
    }

    @OnDisabled
    public void disableClient() {
        this.client = null;
    }

    protected synchronized NuxeoClient buildClient() {
        Interceptor auth = null;
        switch (authType) {
        case TOKEN:
            auth = new TokenAuthInterceptor(credentials);
            break;
        case PORTAL:
            auth = new PortalSSOAuthInterceptor(username, credentials);
            break;
        case BASIC:
        default:
            auth = new BasicAuthInterceptor(username, credentials);
        }

        // Build it
        NuxeoClient client = new NuxeoClient.Builder()
                                                      // Set URL
                                                      .url(serverUrl)
                                                      // Authenticate
                                                      .authentication(auth)
                                                      // Connect
                                                      .build();
        // Ship it
        return client;
    }

    public NuxeoClient getClient() {
        if (this.client != null) {
            return this.client;
        }
        synchronized (this) {
            return this.client = buildClient();
        }
    }

    public String getDefaultRepository() {
        return this.defaultRepo;
    }

}
