/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.nuxeo.labs.nifi.processors;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.nuxeo.client.objects.Document;
import org.nuxeo.client.objects.Documents;
import org.nuxeo.client.spi.NuxeoClientException;

@Tags({ "nuxeo", "execute", "page", "query" })
@CapabilityDescription("Query the Nuxeo repository for a set of related documents.")
@ReadsAttributes({ @ReadsAttribute(attribute = "nx-provider", description = "Page provider to execute"),
        @ReadsAttribute(attribute = "nx-page-size", description = "Page size to retreive"),
        @ReadsAttribute(attribute = "nx-page-index", description = "Page index to start from"),
        @ReadsAttribute(attribute = "nx-max-results", description = "Max results to return"),
        @ReadsAttribute(attribute = "nx-sort-by", description = "Sort by field"),
        @ReadsAttribute(attribute = "nx-sort-order", description = "Sort order to use (ASC, DESC)"),
        @ReadsAttribute(attribute = "nx-query-params", description = "Query parameters to use") })
@WritesAttributes({ @WritesAttribute(attribute = NuxeoAttributes.VAR_DOC_ID, description = "Document ID"),
        @WritesAttribute(attribute = NuxeoAttributes.VAR_ENTITY_TYPE, description = "Document Type") })
@TriggerWhenEmpty
@InputRequirement(Requirement.INPUT_ALLOWED)
public class ExecuteNuxeoPageProvider extends AbstractNuxeoProcessor {

    public static final PropertyDescriptor PROVIDER = new PropertyDescriptor.Builder().name("PROVIDER")
                                                                                      .displayName("Provider")
                                                                                      .description(
                                                                                              "Page provider to execute. {nx-provider}")
                                                                                      .expressionLanguageSupported(
                                                                                              ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                                                                                      .required(false)
                                                                                      .addValidator(Validator.VALID)
                                                                                      .build();

    public static final PropertyDescriptor PAGE_SIZE = new PropertyDescriptor.Builder().name("PAGE_SIZE")
                                                                                       .displayName("Page Size")
                                                                                       .description(
                                                                                               "Page size to retrieve. {nx-page-size}")
                                                                                       .expressionLanguageSupported(
                                                                                               ExpressionLanguageScope.NONE)
                                                                                       .defaultValue("10")
                                                                                       .required(false)
                                                                                       .addValidator(
                                                                                               StandardValidators.INTEGER_VALIDATOR)
                                                                                       .build();

    public static final PropertyDescriptor PAGE_INDEX = new PropertyDescriptor.Builder().name("PAGE_INDEX")
                                                                                        .displayName("Page Index")
                                                                                        .description(
                                                                                                "Result page index. {nx-page-index}")
                                                                                        .expressionLanguageSupported(
                                                                                                ExpressionLanguageScope.NONE)
                                                                                        .defaultValue("0")
                                                                                        .required(false)
                                                                                        .addValidator(
                                                                                                StandardValidators.INTEGER_VALIDATOR)
                                                                                        .build();

    public static final PropertyDescriptor MAX_RESULTS = new PropertyDescriptor.Builder().name("MAX_RESULTS")
                                                                                         .displayName("Max Results")
                                                                                         .description(
                                                                                                 "Max results to return. {nx-max-results}")
                                                                                         .expressionLanguageSupported(
                                                                                                 ExpressionLanguageScope.NONE)
                                                                                         .defaultValue("1000")
                                                                                         .required(false)
                                                                                         .addValidator(
                                                                                                 StandardValidators.INTEGER_VALIDATOR)
                                                                                         .build();

    public static final PropertyDescriptor SORT_BY = new PropertyDescriptor.Builder().name("SORT_BY")
                                                                                     .displayName("Sort By")
                                                                                     .description(
                                                                                             "Sort by field. {nx-sort-by}")
                                                                                     .expressionLanguageSupported(
                                                                                             ExpressionLanguageScope.NONE)
                                                                                     .required(false)
                                                                                     .addValidator(Validator.VALID)
                                                                                     .build();

    public static final PropertyDescriptor SORT_ORDER = new PropertyDescriptor.Builder().name("SORT_ORDER")
                                                                                        .displayName("Sort Order")
                                                                                        .description(
                                                                                                "Sort order to use (ASC, DESC). {nx-sort-order}")
                                                                                        .expressionLanguageSupported(
                                                                                                ExpressionLanguageScope.NONE)
                                                                                        .required(false)
                                                                                        .addValidator(Validator.VALID)
                                                                                        .build();

    public static final PropertyDescriptor QUERY_PARAMS = new PropertyDescriptor.Builder().name("QUERY_PARAMS")
                                                                                          .displayName(
                                                                                                  "Query Parameters")
                                                                                          .description(
                                                                                                  "Query parameters to use. {nx-query-params}")
                                                                                          .expressionLanguageSupported(
                                                                                                  ExpressionLanguageScope.NONE)
                                                                                          .required(false)
                                                                                          .addValidator(Validator.VALID)
                                                                                          .build();

    public static final Relationship REL_NEXT_PAGE = new Relationship.Builder().name("NextPage")
                                                                               .description("Next Page for Query")
                                                                               .build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(NUXEO_CLIENT_SERVICE);
        descriptors.add(TARGET_REPO);
        descriptors.add(PROVIDER);
        descriptors.add(PAGE_SIZE);
        descriptors.add(PAGE_INDEX);
        descriptors.add(MAX_RESULTS);
        descriptors.add(SORT_BY);
        descriptors.add(SORT_ORDER);
        descriptors.add(QUERY_PARAMS);
        descriptors.add(FILTER_SCHEMAS);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_NEXT_PAGE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            // Create dummy for externally-triggered event (time, etc)
            flowFile = session.create();
        }

        // Extract arguments
        String providerName = getArg(context, flowFile, "nx-provider", PROVIDER);
        String pageSize = getArg(context, flowFile, "nx-page-size", PAGE_SIZE);
        String currentPageIndex = getArg(context, flowFile, "nx-page-index", PAGE_INDEX);
        String maxResults = getArg(context, flowFile, "nx-max-results", MAX_RESULTS);
        String sortBy = getArg(context, flowFile, "nx-sort-by", SORT_BY);
        String sortOrder = getArg(context, flowFile, "nx-sort-order", SORT_ORDER);
        String queryParams = getArg(context, flowFile, "nx-query-params", QUERY_PARAMS);

        // Evaluate target path
        try {
            // Invoke document query operation
            Documents docs = getRepository(context, flowFile).queryByProvider(providerName, pageSize, currentPageIndex,
                    maxResults, sortBy, sortOrder, queryParams);

            // Check errors...
            if (docs.hasError()) {
                // Reset parameters to those provided by config or input
                session.putAttribute(flowFile, "nx-provider", providerName);
                session.putAttribute(flowFile, "nx-page-size", pageSize);
                session.putAttribute(flowFile, "nx-page-index", currentPageIndex);
                session.putAttribute(flowFile, "nx-max-results", maxResults);
                if (sortBy != null) {
                    session.putAttribute(flowFile, "nx-sort-by", sortBy);
                }
                if (sortOrder != null) {
                    session.putAttribute(flowFile, "nx-sort-order", sortOrder);
                }
                if (queryParams != null) {
                    session.putAttribute(flowFile, "nx-query-params", queryParams);
                }
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            // Create next page flowfile
            if (docs.isNextPageAvailable()) {
                FlowFile nextPage = session.create(flowFile);
                // Get next page parameters provided by input
                session.putAttribute(nextPage, "nx-provider", providerName);
                session.putAttribute(nextPage, "nx-page-size", Integer.toString(docs.getPageSize()));
                session.putAttribute(nextPage, "nx-page-index", Integer.toString(docs.getCurrentPageIndex() + 1));
                session.putAttribute(nextPage, "nx-max-results", Integer.toString(docs.getResultsCount()));
                if (sortBy != null) {
                    session.putAttribute(nextPage, "nx-sort-by", sortBy);
                }
                if (sortOrder != null) {
                    session.putAttribute(nextPage, "nx-sort-order", sortOrder);
                }
                if (queryParams != null) {
                    session.putAttribute(nextPage, "nx-query-params", queryParams);
                }
                session.transfer(nextPage, REL_NEXT_PAGE);
            }

            // Write documents to flowfile
            for (Document doc : docs.getDocuments()) {
                FlowFile childFlow = session.create(flowFile);
                session.putAttribute(childFlow, VAR_ENTITY_TYPE, doc.getEntityType());
                session.putAttribute(childFlow, VAR_DOC_ID, doc.getId());

                // Convert and write to JSON
                String json = this.nuxeoClient.getConverterFactory().writeJSON(doc);
                try (OutputStream out = session.write(childFlow)) {
                    IOUtils.write(json, out, UTF8);
                } catch (IOException e) {
                    session.putAttribute(flowFile, VAR_ERROR, e.getMessage());
                    session.transfer(flowFile, REL_FAILURE);
                    continue;
                }

                session.transfer(childFlow, REL_SUCCESS);
            }
        } catch (NuxeoClientException nce) {
            getLogger().error("Unable to query repository with provider: " + providerName, nce);
            session.putAttribute(flowFile, VAR_ERROR, nce.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        } finally {
            session.transfer(flowFile, REL_ORIGINAL);
        }
    }
}
