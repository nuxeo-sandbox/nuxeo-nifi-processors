package org.nuxeo.labs.nifi.processors;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;

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

    public static final PropertyDescriptor TARGET_PATH = new PropertyDescriptor.Builder().name("TARGET_PATH")
                                                                                         .displayName("Target Path")
                                                                                         .description(
                                                                                                 "Target Path to use.")
                                                                                         .expressionLanguageSupported(
                                                                                                 ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                                                                                         .required(false)
                                                                                         .addValidator(
                                                                                                 StandardValidators.NON_BLANK_VALIDATOR)
                                                                                         .build();

    public static final PropertyDescriptor TARGET_TYPE = new PropertyDescriptor.Builder().name("TARGET_TYPE")
                                                                                         .displayName("Target Type")
                                                                                         .description(
                                                                                                 "Target Document Type to use.")
                                                                                         .expressionLanguageSupported(
                                                                                                 ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                                                                                         .required(true)
                                                                                         .addValidator(
                                                                                                 StandardValidators.NON_BLANK_VALIDATOR)
                                                                                         .build();

    public static final PropertyDescriptor TARGET_TITLE = new PropertyDescriptor.Builder().name("TARGET_TITLE")
                                                                                          .displayName("Target Title")
                                                                                          .description(
                                                                                                  "Target Title to use.")
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
                                                                                            .addValidator(
                                                                                                    StandardValidators.createListValidator(
                                                                                                            true, true,
                                                                                                            StandardValidators.ATTRIBUTE_KEY_VALIDATOR))
                                                                                            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("SUCCESS")
                                                                             .description("Document retrieved")
                                                                             .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder().name("ORIGINAL")
                                                                              .description("Original Document")
                                                                              .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder().name("FAILURE")
                                                                             .description("Document unavailable")
                                                                             .build();

    protected static final Charset UTF8 = Charset.forName("UTF-8");

    protected NuxeoClientService nuxeoClientService;

    protected NuxeoClient nuxeoClient;

    protected List<PropertyDescriptor> descriptors;

    protected Set<Relationship> relationships;

    protected NuxeoClient getClient(final ProcessContext context) {
        this.nuxeoClientService = context.getProperty(NUXEO_CLIENT_SERVICE)
                                         .asControllerService(NuxeoClientService.class);
        return this.nuxeoClientService.getClient();
    }

    /**
     * Retrieve the Repostiory from the Nuxeo Client connection. Requires {@link NUXEO_CLIENT_SERVICE} and
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

        // TODO: specify schemas per processor
        if (repo == null) {
            return this.nuxeoClient.schemas("*").repository();
        } else {
            return this.nuxeoClient.schemas("*").repository(repo);
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
    }

    protected String getArg(ProcessContext ctx, FlowFile ff, String key, PropertyDescriptor desc) {
        if (key != null && ff.getAttribute(key) != null) {
            return ff.getAttribute(key);
        }
        if (desc != null) {
            PropertyValue pdv = ctx.getProperty(desc);
            if (pdv.isSet()) {
                PropertyValue val = desc.isExpressionLanguageSupported() ? pdv.evaluateAttributeExpressions(ff) : pdv;
                return val.getValue();
            }
        }
        return null;
    }

    protected Document getDocument(ProcessContext context, FlowFile flowFile) {
        String docId = getArg(context, flowFile, DOC_ID, null);
        String path = getArg(context, flowFile, PATH, TARGET_PATH);

        Repository rep = getRepository(context);
        Document doc = docId != null ? rep.fetchDocumentById(docId) : rep.fetchDocumentByPath(path);
        return doc;
    }

}
