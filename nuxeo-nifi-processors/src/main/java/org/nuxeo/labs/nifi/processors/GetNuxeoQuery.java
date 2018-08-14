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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
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

@Tags({ "nuxeo", "get", "query" })
@CapabilityDescription("Query the Nuxeo repository for a set of related documents.")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "nx-query", description = "NXQL to execute"),
        @ReadsAttribute(attribute = "nx-page-size", description = "Pagqe size to retreive"),
        @ReadsAttribute(attribute = "nx-page-index", description = "Page index to start from"),
        @ReadsAttribute(attribute = "nx-max-results", description = "Max results to return"),
        @ReadsAttribute(attribute = "nx-sort-by", description = "Sort by field"),
        @ReadsAttribute(attribute = "nx-sort-order", description = "Sort order to use (ASC, DESC)"),
        @ReadsAttribute(attribute = "nx-query-params", description = "Query parameters to use") })
@WritesAttributes({ @WritesAttribute(attribute = "nx-docid", description = "Document ID") })
@TriggerWhenEmpty
public class GetNuxeoQuery extends AbstractNuxeoProcessor {

    public static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder().name("QUERY")
                                                                                   .displayName("Query")
                                                                                   .description(
                                                                                           "NXQL to execute, nx-query overrides.")
                                                                                   .expressionLanguageSupported(
                                                                                           ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                                                                                   .required(false)
                                                                                   .addValidator(Validator.VALID)
                                                                                   .build();

    public static final PropertyDescriptor PAGE_SIZE = new PropertyDescriptor.Builder().name("PAGE_SIZE")
                                                                                       .displayName("Page Size")
                                                                                       .description(
                                                                                               "Page size to retrieve, nx-page-size is used.")
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
                                                                                                "Page index to start from, nx-page-index overrides.")
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
                                                                                                 "Max results to return, nx-max-results overrides.")
                                                                                         .expressionLanguageSupported(
                                                                                                 ExpressionLanguageScope.NONE)
                                                                                         .defaultValue("10")
                                                                                         .required(false)
                                                                                         .addValidator(
                                                                                                 StandardValidators.INTEGER_VALIDATOR)
                                                                                         .build();

    public static final PropertyDescriptor SORT_BY = new PropertyDescriptor.Builder().name("SORT_BY")
                                                                                     .displayName("Sort By")
                                                                                     .description(
                                                                                             "Sort by field, nx-sort-by overrides.")
                                                                                     .expressionLanguageSupported(
                                                                                             ExpressionLanguageScope.NONE)
                                                                                     .required(false)
                                                                                     .addValidator(Validator.VALID)
                                                                                     .build();

    public static final PropertyDescriptor SORT_ORDER = new PropertyDescriptor.Builder().name("SORT_ORDER")
                                                                                        .displayName("Sort Order")
                                                                                        .description(
                                                                                                "Sort order to use (ASC, DESC), nx-sort-order overrides.")
                                                                                        .expressionLanguageSupported(
                                                                                                ExpressionLanguageScope.NONE)
                                                                                        .required(false)
                                                                                        .addValidator(Validator.VALID)
                                                                                        .build();

    public static final PropertyDescriptor QUERY_PARAMS = new PropertyDescriptor.Builder().name("QUERY_PARAMS")
                                                                                          .displayName(
                                                                                                  "Query Parameters")
                                                                                          .description(
                                                                                                  "Query parameters to use, nx-query-params overrides.")
                                                                                          .expressionLanguageSupported(
                                                                                                  ExpressionLanguageScope.NONE)
                                                                                          .required(false)
                                                                                          .addValidator(Validator.VALID)
                                                                                          .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(NUXEO_CLIENT_SERVICE);
        descriptors.add(TARGET_REPO);
        descriptors.add(QUERY);
        descriptors.add(PAGE_SIZE);
        descriptors.add(PAGE_INDEX);
        descriptors.add(MAX_RESULTS);
        descriptors.add(SORT_BY);
        descriptors.add(SORT_ORDER);
        descriptors.add(QUERY_PARAMS);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_ORIGINAL);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            // Create dummy
            flowFile = session.create();
        }

        // Extract arguments
        String query = getArg(context, flowFile, "nx-query", QUERY);
        String pageSize = getArg(context, flowFile, "nx-page-size", PAGE_SIZE);
        String currentPageIndex = getArg(context, flowFile, "nx-page-index", PAGE_INDEX);
        String maxResults = getArg(context, flowFile, "nx-max-results", MAX_RESULTS);
        String sortBy = getArg(context, flowFile, "nx-sort-by", SORT_BY);
        String sortOrder = getArg(context, flowFile, "nx-sort-order", SORT_ORDER);
        String queryParams = getArg(context, flowFile, "nx-query-params", QUERY_PARAMS);

        // Evaluate target path
        try {
            // Invoke document query operation
            Documents docs = getRepository(context).query(query, pageSize, currentPageIndex, maxResults, sortBy,
                    sortOrder, queryParams);

            // Write documents to flowfile
            for (Document doc : docs.getDocuments()) {
                FlowFile childFlow = session.create(flowFile);

                // Convert and write to JSON
                String json = this.nuxeoClient.getConverterFactory().writeJSON(doc);
                try (OutputStream out = session.write(childFlow)) {
                    IOUtils.write(json, out, Charset.forName("UTF-8"));
                } catch (IOException e) {
                    continue;
                }
                session.putAttribute(childFlow, "nx-docid", doc.getId());
                session.transfer(childFlow, REL_SUCCESS);
            }
        } catch (NuxeoClientException nce) {
            getLogger().error("Unable to query repository: " + query, nce);
        }

        // Send original
        session.transfer(flowFile, REL_ORIGINAL);
    }
}
