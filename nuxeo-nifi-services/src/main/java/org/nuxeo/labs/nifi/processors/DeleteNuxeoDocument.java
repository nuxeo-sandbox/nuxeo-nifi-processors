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
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
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
import org.apache.nifi.processor.util.StandardValidators;
import org.nuxeo.client.objects.Document;
import org.nuxeo.client.objects.Repository;
import org.nuxeo.client.spi.NuxeoClientException;

@Tags({ "nuxeo", "delete", "document" })
@CapabilityDescription("Remove a document from Nuxeo.")
@SeeAlso({ GetNuxeoDocument.class, GetNuxeoBlob.class })
@ReadsAttributes({ @ReadsAttribute(attribute = NuxeoAttributes.VAR_DOC_ID, description = "Document ID {nx-docid}"),
        @ReadsAttribute(attribute = NuxeoAttributes.VAR_PATH, description = "Document Path {nx-path}") })
@WritesAttributes({
        @WritesAttribute(attribute = NuxeoAttributes.VAR_DOC_ID, description = "Added for each document deleted"),
        @WritesAttribute(attribute = "nx-trashed", description = "True if document trashed"),
        @WritesAttribute(attribute = NuxeoAttributes.VAR_ERROR, description = "Error set if problem occurs") })
@InputRequirement(Requirement.INPUT_REQUIRED)
public class DeleteNuxeoDocument extends AbstractNuxeoProcessor {

    public static final PropertyDescriptor TRASH_DOCUMENT = new PropertyDescriptor.Builder().name("TRASH_DOCUMENT")
                                                                                            .displayName(
                                                                                                    "Trash Document")
                                                                                            .description(
                                                                                                    "Move document to the trash.  If 'No', permanently removes document")
                                                                                            .allowableValues(YES, NO)
                                                                                            .defaultValue("false")
                                                                                            .required(true)
                                                                                            .addValidator(
                                                                                                    StandardValidators.BOOLEAN_VALIDATOR)
                                                                                            .build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(NUXEO_CLIENT_SERVICE);
        descriptors.add(TARGET_REPO);
        descriptors.add(DOC_PATH);
        descriptors.add(TRASH_DOCUMENT);
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

        boolean useTrash = context.getProperty(TRASH_DOCUMENT).asBoolean();

        try {
            // Invoke document operation
            Document doc = getDocument(context, flowFile);
            if (doc == null) {
                return;
            }
            Repository rep = getRepository(context, flowFile);
            session.putAttribute(flowFile, VAR_DOC_ID, doc.getId());

            if (useTrash) {
                doc = doc.trash();
                session.putAttribute(flowFile, "nx-trashed", "true");

                // Convert and write to JSON
                String json = nxClient().getConverterFactory().writeJSON(doc);
                try (OutputStream out = session.write(flowFile)) {
                    IOUtils.write(json, out, UTF8);
                } catch (IOException e) {
                    session.putAttribute(flowFile, VAR_ERROR, e.getMessage());
                    session.transfer(flowFile, REL_FAILURE);
                    return;
                }
            } else {
                // Remove document
                rep.deleteDocument(doc);
            }

            session.transfer(flowFile, REL_SUCCESS);
        } catch (NuxeoClientException nce) {
            session.putAttribute(flowFile, VAR_ERROR, nce.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
