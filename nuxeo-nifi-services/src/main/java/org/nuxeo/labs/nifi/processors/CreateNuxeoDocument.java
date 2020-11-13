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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.nuxeo.client.objects.Document;
import org.nuxeo.client.spi.NuxeoClientException;

@Tags({ "nuxeo", "create", "document" })
@CapabilityDescription("Create a Nuxeo Document in the repository.")
@SeeAlso({ GetNuxeoDocument.class, UpdateNuxeoDocument.class })
@ReadsAttributes({ @ReadsAttribute(attribute = NuxeoAttributes.VAR_NAME, description = "Document name"),
        @ReadsAttribute(attribute = NuxeoAttributes.VAR_PATH, description = "Document path"),
        @ReadsAttribute(attribute = NuxeoAttributes.VAR_TYPE, description = "Document type (File, Picture, etc)"),
        @ReadsAttribute(attribute = NuxeoAttributes.VAR_TITLE, description = "Document title") })
@WritesAttributes({ @WritesAttribute(attribute = NuxeoAttributes.VAR_ENTITY_TYPE, description = "Document entity type"),
        @WritesAttribute(attribute = NuxeoAttributes.VAR_DOC_ID, description = "Document ID") })
@InputRequirement(Requirement.INPUT_ALLOWED)
public class CreateNuxeoDocument extends AbstractNuxeoDynamicProcessor {

    public static final PropertyDescriptor DOC_NAME = new PropertyDescriptor.Builder().name("DOC_NAME")
                                                                                      .displayName("Document Name")
                                                                                      .description(
                                                                                              "Document Name to use. {nx-name}")
                                                                                      .expressionLanguageSupported(
                                                                                              ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                                                                                      .required(true)
                                                                                      .addValidator(
                                                                                              StandardValidators.NON_BLANK_VALIDATOR)
                                                                                      .build();

    public static final PropertyDescriptor ATTACH_BLOB = new PropertyDescriptor.Builder().name("ATTACH_BLOB")
                                                                                         .displayName("Attach Blob")
                                                                                         .description(
                                                                                                 "Attach uplaoded blob to document.")
                                                                                         .allowableValues(YES, NO)
                                                                                         .defaultValue("false")
                                                                                         .required(true)
                                                                                         .addValidator(
                                                                                                 StandardValidators.BOOLEAN_VALIDATOR)
                                                                                         .build();

    public static final PropertyDescriptor XPATH = new PropertyDescriptor.Builder().name("XPATH")
                                                                                   .displayName("Blob X-Path")
                                                                                   .description(
                                                                                           "Document x-path blob property to update. {nx-xpath}")
                                                                                   .expressionLanguageSupported(
                                                                                           ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                                                                                   .defaultValue(
                                                                                           Document.DEFAULT_FILE_CONTENT)
                                                                                   .required(false)
                                                                                   .addValidator(
                                                                                           StandardValidators.NON_BLANK_VALIDATOR)
                                                                                   .build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(NUXEO_CLIENT_SERVICE);
        descriptors.add(TARGET_REPO);
        descriptors.add(DOC_PATH);
        descriptors.add(DOC_NAME);
        descriptors.add(DOC_TYPE);
        descriptors.add(DOC_TITLE);
        descriptors.add(ATTACH_BLOB);
        descriptors.add(XPATH);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        // Evaluate target path
        String path = getArg(context, flowFile, VAR_PATH, DOC_PATH);
        String name = getArg(context, flowFile, VAR_NAME, DOC_NAME);
        String type = getArg(context, flowFile, VAR_TYPE, DOC_TYPE);
        String title = getArg(context, flowFile, VAR_TITLE, DOC_TITLE);
        if (title == null) {
            title = name;
        }

        try {
            Document doc = Document.createWithName(name, type);
            doc.setPropertyValue("dc:title", title);

            // Set the new properties
            if (this.dynamicProperties != null) {
                for (PropertyDescriptor desc : this.dynamicProperties) {
                    String key = desc.getName();
                    String value = getArg(context, flowFile, null, desc);
                    if (value != null) {
                        Object json = isMaybeJSON(value);
                        if (json != null) {
                            doc.setPropertyValue(key, json);
                        } else {
                            doc.setPropertyValue(key, value);
                        }
                    }
                }
            }

            // Attach blob?
            PropertyValue attach = getValue(context, flowFile, ATTACH_BLOB);
            if (attach != null && attach.asBoolean()) {
                // Blob properties
                Map<String, Object> props = new HashMap<>();
                String xpath = getArg(context, flowFile, VAR_XPATH, XPATH);
                String batch = getArg(context, flowFile, VAR_BATCH, null);
                String index = getArg(context, flowFile, VAR_INDEX, null);

                if (xpath != null && batch != null && index != null) {
                    props.put("upload-batch", batch);
                    props.put("upload-fileId", index);

                    // Attach the blob
                    doc.setPropertyValue(xpath, props);
                }
            }

            // Create document
            doc = getRepository(context).createDocumentByPath(path, doc);

            // Convert and write to JSON
            String json = this.nuxeoClient.getConverterFactory().writeJSON(doc);
            try (OutputStream out = session.write(flowFile)) {
                IOUtils.write(json, out, UTF8);
            } catch (IOException e) {
                session.putAttribute(flowFile, VAR_ERROR, e.getMessage());
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            session.putAttribute(flowFile, VAR_ENTITY_TYPE, doc.getEntityType());
            session.putAttribute(flowFile, VAR_DOC_ID, doc.getId());
            session.transfer(flowFile, REL_SUCCESS);
        } catch (NuxeoClientException nce) {
            getLogger().error("Unable to store document", nce);
            session.putAttribute(flowFile, VAR_ERROR, String.valueOf(nce));
            session.transfer(flowFile, REL_FAILURE);
        }
    }

}
