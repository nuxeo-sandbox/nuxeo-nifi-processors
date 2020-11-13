package org.nuxeo.labs.nifi.processors;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.nuxeo.client.NuxeoClient;
import org.nuxeo.client.objects.Document;
import org.nuxeo.client.objects.Repository;
import org.nuxeo.labs.nifi.NuxeoClientService;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public abstract class AbstractNuxeoProcessor extends AbstractProcessor implements NuxeoAttributes {

    public static final PropertyDescriptor NUXEO_CLIENT_SERVICE = new PropertyDescriptor.Builder().name(
            "CLIENT_SERVICE")
                                                                                                  .displayName(
                                                                                                          "Nuxeo Client Service")
                                                                                                  .description(
                                                                                                          "Nuxeo Client configuration")
                                                                                                  .addValidator(
                                                                                                          Validator.VALID)
                                                                                                  .required(true)
                                                                                                  .identifiesControllerService(
                                                                                                          NuxeoClientService.class)
                                                                                                  .build();

    public static final PropertyDescriptor TARGET_REPO = new PropertyDescriptor.Builder().name("TARGET_REPO")
                                                                                         .displayName(
                                                                                                 "Target Repository")
                                                                                         .description(
                                                                                                 "Target Repository to use.")
                                                                                         .expressionLanguageSupported(
                                                                                                 ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                                                                                         .required(false)
                                                                                         .addValidator(Validator.VALID)
                                                                                         .build();

    public static final PropertyDescriptor DOC_ID = new PropertyDescriptor.Builder().name("DOC_ID")
                                                                                    .displayName("Document ID")
                                                                                    .description(
                                                                                            "Document identifier to use. {nx-docid}")
                                                                                    .expressionLanguageSupported(
                                                                                            ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                                                                                    .required(false)
                                                                                    .addValidator(
                                                                                            StandardValidators.NON_BLANK_VALIDATOR)
                                                                                    .build();

    public static final PropertyDescriptor DOC_PATH = new PropertyDescriptor.Builder().name("DOC_PATH")
                                                                                      .displayName("Document Path")
                                                                                      .description(
                                                                                              "Document Path to use. {nx-path}")
                                                                                      .expressionLanguageSupported(
                                                                                              ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                                                                                      .required(false)
                                                                                      .addValidator(
                                                                                              StandardValidators.NON_BLANK_VALIDATOR)
                                                                                      .build();

    public static final PropertyDescriptor DOC_TYPE = new PropertyDescriptor.Builder().name("DOC_TYPE")
                                                                                      .displayName("Document Type")
                                                                                      .description(
                                                                                              "Document Type to use. {nx-type}")
                                                                                      .expressionLanguageSupported(
                                                                                              ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                                                                                      .required(true)
                                                                                      .addValidator(
                                                                                              StandardValidators.NON_BLANK_VALIDATOR)
                                                                                      .build();

    public static final PropertyDescriptor DOC_TITLE = new PropertyDescriptor.Builder().name("DOC_TITLE")
                                                                                       .displayName("Document Title")
                                                                                       .description(
                                                                                               "Document Title to use. {nx-title}")
                                                                                       .expressionLanguageSupported(
                                                                                               ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                                                                                       .required(false)
                                                                                       .addValidator(
                                                                                               StandardValidators.NON_BLANK_VALIDATOR)
                                                                                       .build();

    public static final PropertyDescriptor FILTER_SCHEMAS = new PropertyDescriptor.Builder().name("FILTER_SCHEMAS")
                                                                                            .displayName(
                                                                                                    "Include Schemas")
                                                                                            .description(
                                                                                                    "Only include the specified schema attributes.")
                                                                                            .required(false)
                                                                                            .defaultValue("*")
                                                                                            .addValidator(
                                                                                                    StandardValidators.createListValidator(
                                                                                                            true, true,
                                                                                                            StandardValidators.ATTRIBUTE_KEY_VALIDATOR))
                                                                                            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
                                                                             .description("Document retrieved")
                                                                             .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder().name("original")
                                                                              .description("Original Document")
                                                                              .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
                                                                             .description("Document unavailable")
                                                                             .build();

    protected static final Charset UTF8 = Charset.forName("UTF-8");

    protected NuxeoClientService nuxeoClientService;

    protected NuxeoClient nuxeoClient;

    protected List<PropertyDescriptor> descriptors;

    protected Set<Relationship> relationships;

    private ObjectMapper objectMapper;

    protected NuxeoClient getClient(final ProcessContext context) {
        this.nuxeoClientService = context.getProperty(NUXEO_CLIENT_SERVICE)
                                         .asControllerService(NuxeoClientService.class);
        return this.nuxeoClientService.getClient();
    }

    /**
     * Retrieve the Repository from the Nuxeo Client connection. Requires {@link NUXEO_CLIENT_SERVICE} and
     * {@link TARGET_REPO} properties within processor descriptor.
     * 
     * @param context
     * @return
     */
    protected Repository getRepository(final ProcessContext context) {
        String repo = this.nuxeoClientService.getDefaultRepository();

        PropertyValue pval = context.getProperty(TARGET_REPO);
        if (pval.isSet()) {
            repo = pval.getValue();
        }

        String schemas = "*";
        if (this.descriptors.contains(FILTER_SCHEMAS)) {
            schemas = context.getProperty(FILTER_SCHEMAS).getValue();
        }

        NuxeoClient client = this.nuxeoClient.schemas(schemas);
        if (repo == null) {
            return client.repository();
        } else {
            return client.repository(repo);
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.descriptors;
    }

    protected void processorScheduled(final ProcessContext context) {
        // no-op
    }

    protected void processorStopped(final ProcessContext context) {
        // no-op
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        if (this.nuxeoClient != null) {
            onStopped(context);
        }
        this.nuxeoClient = getClient(context);
        processorScheduled(context);
    }

    @OnStopped
    public void onStopped(final ProcessContext context) {
        processorStopped(context);

        if (this.nuxeoClient != null) {
            this.nuxeoClient.disconnect();
        }
        this.nuxeoClient = null;
        this.nuxeoClientService = null;
        this.objectMapper = null;
    }

    protected String getArg(ProcessContext ctx, FlowFile ff, String key, PropertyDescriptor desc) {
        if (desc != null) {
            PropertyValue pdv = ctx.getProperty(desc);
            if (pdv.isSet()) {
                if (desc.isExpressionLanguageSupported()) {
                    if (ff == null) {
                        getLogger().error("Attribute expression requires flowfile for descriptor: " + desc);
                    }
                    pdv = pdv.evaluateAttributeExpressions(ff);
                }
                return pdv.getValue();
            }
        }
        if (key != null && ff != null) {
            return ff.getAttribute(key);
        }
        return null;
    }

    protected PropertyValue getValue(ProcessContext ctx, FlowFile ff, PropertyDescriptor desc) {
        if (desc != null) {
            PropertyValue pdv = ctx.getProperty(desc);
            if (pdv.isSet()) {
                PropertyValue val = desc.isExpressionLanguageSupported() ? pdv.evaluateAttributeExpressions(ff) : pdv;
                return val;
            }
        }
        return null;
    }

    protected Document getDocument(ProcessContext context, FlowFile flowFile) {
        String docId = getArg(context, flowFile, VAR_DOC_ID, DOC_ID);
        String path = getArg(context, flowFile, VAR_PATH, DOC_PATH);

        if (StringUtils.isBlank(docId) && StringUtils.isBlank(path)) {
            return null;
        }

        Repository rep = getRepository(context);
        Document doc = docId != null ? rep.fetchDocumentById(docId) : rep.fetchDocumentByPath(path);
        return doc;
    }

    protected JsonNode isMaybeJSON(String val) {
        // Expensive but guaranteed to work with valid JSON
        try {
            final ObjectMapper mapper = objectMapper();
            return mapper.readTree(val);
        } catch (IOException e) {
            return null;
        }
    }

    protected ObjectMapper objectMapper() {
        if (this.objectMapper != null) {
            return this.objectMapper;
        }
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.objectMapper.registerModule(new JavaTimeModule());
        this.objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        return this.objectMapper;
    }

}
