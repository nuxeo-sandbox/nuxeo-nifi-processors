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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.nuxeo.client.objects.Document;
import org.nuxeo.client.objects.EntityTypes;
import org.nuxeo.client.spi.NuxeoClientException;

@Tags({ "nuxeo", "put", "attach", "blob" })
@CapabilityDescription("Attach a blob to a Nuxeo Document.")
@SeeAlso({ UploadNuxeoBlob.class })
@ReadsAttributes({ @ReadsAttribute(attribute = NuxeoAttributes.VAR_BATCH, description = "Upload batch identifier"),
        @ReadsAttribute(attribute = NuxeoAttributes.VAR_INDEX, description = "Upload index"),
        @ReadsAttribute(attribute = NuxeoAttributes.VAR_XPATH, description = "Property XPath"),
        @ReadsAttribute(attribute = NuxeoAttributes.VAR_ENTITY_TYPE, description = "Document entity type") })
@WritesAttributes({ @WritesAttribute(attribute = NuxeoAttributes.VAR_ENTITY_TYPE, description = "Document entity type"),
        @WritesAttribute(attribute = NuxeoAttributes.VAR_DOC_ID, description = "Document ID") })
@InputRequirement(Requirement.INPUT_REQUIRED)
public class AttachNuxeoBlob extends AbstractNuxeoDynamicProcessor {

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
        descriptors.add(DOC_ID);
        descriptors.add(DOC_PATH);
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

        String xpath = getArg(context, flowFile, VAR_XPATH, XPATH);
        String batch = getArg(context, flowFile, VAR_BATCH, null);
        String index = getArg(context, flowFile, VAR_INDEX, null);

        if (StringUtils.isBlank(batch) || StringUtils.isBlank(index)) {
            getLogger().error("No upload batch found");
            session.putAttribute(flowFile, VAR_ERROR, "No upload batch");
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        try {
            Document doc = null;

            // Try to load from existing context
            String entityType = flowFile.getAttribute(VAR_ENTITY_TYPE);
            if (EntityTypes.DOCUMENT.equals(entityType)) {
                try (InputStream in = session.read(flowFile)) {
                    String json = IOUtils.toString(in, UTF8);
                    doc = nxClient().getConverterFactory().readJSON(json, Document.class);
                    doc.reconnectWith(nxClient());
                } catch (Exception iox) {
                    getLogger().warn("Unable to load document from existing resource", iox);
                }
            }

            // Load from server
            if (doc == null) {
                doc = getDocument(context, flowFile);
            }

            // Blob properties
            Map<String, Object> props = new HashMap<>();
            props.put("upload-batch", batch);
            props.put("upload-fileId", index);

            // Attach the blob
            doc.setPropertyValue(xpath, props);
            doc = doc.updateDocument();

            session.putAttribute(flowFile, VAR_ENTITY_TYPE, doc.getEntityType());
            session.putAttribute(flowFile, VAR_DOC_ID, doc.getId());
            session.transfer(flowFile, REL_SUCCESS);
        } catch (NuxeoClientException nce) {
            getLogger().error("Unable to attach document", nce);
            session.putAttribute(flowFile, VAR_ERROR, String.valueOf(nce));
            session.transfer(flowFile, REL_FAILURE);
        }
    }

}
