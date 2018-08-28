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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.nuxeo.client.objects.Document;
import org.nuxeo.client.objects.EntityTypes;
import org.nuxeo.client.spi.NuxeoClientException;

@Tags({ "nuxeo", "put", "document" })
@CapabilityDescription("Update a Nuxeo Document in the repository.")
@SeeAlso({ GetNuxeoDocument.class })
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = NuxeoAttributes.DOC_ID, description = "Document ID") })
public class UpdateNuxeoDocument extends AbstractNuxeoDynamicProcessor {

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(NUXEO_CLIENT_SERVICE);
        descriptors.add(TARGET_REPO);
        descriptors.add(TARGET_PATH);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        if (!propertyDescriptorName.matches(PROP_KEY_PATTERN)) {
            return null;
        }
        return super.getSupportedDynamicPropertyDescriptor(propertyDescriptorName);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            Document doc = null;

            // Try to load from existing context
            String entityType = flowFile.getAttribute(ENTITY_TYPE);
            if (EntityTypes.DOCUMENT.equals(entityType)) {
                try (InputStream in = session.read(flowFile)) {
                    String json = IOUtils.toString(in, UTF8);
                    doc = this.nuxeoClient.getConverterFactory().readJSON(json, Document.class);
                    doc.reconnectWith(this.nuxeoClient);
                } catch (Exception iox) {
                    getLogger().warn("Unable to load document from existing resource", iox);
                }
            }

            // Load from server
            if (doc == null) {
                doc = getDocument(context, flowFile);
            }

            // Set the new properties
            if (this.dynamicProperties != null) {
                for (PropertyDescriptor desc : this.dynamicProperties) {
                    String key = desc.getName();
                    String value = getArg(context, flowFile, null, desc);
                    doc.setPropertyValue(key, value);
                }
            }

            // Update the document
            doc = doc.updateDocument();

            session.putAttribute(flowFile, ENTITY_TYPE, doc.getEntityType());
            session.putAttribute(flowFile, DOC_ID, doc.getId());
            session.transfer(flowFile, REL_SUCCESS);
        } catch (NuxeoClientException nce) {
            getLogger().error("Unable to store document", nce);
            session.putAttribute(flowFile, ERROR, String.valueOf(nce));
            session.transfer(flowFile, REL_FAILURE);
        }
    }

}
