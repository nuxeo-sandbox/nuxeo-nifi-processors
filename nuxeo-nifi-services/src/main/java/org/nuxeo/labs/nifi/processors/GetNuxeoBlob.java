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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
import org.nuxeo.client.objects.Repository;
import org.nuxeo.client.objects.blob.StreamBlob;
import org.nuxeo.client.spi.NuxeoClientException;

@Tags({ "nuxeo", "get", "blob" })
@CapabilityDescription("Retrieve the blob data from Nuxeo for a given document.")
@SeeAlso({ NuxeoBlobOperation.class, UploadNuxeoBlob.class })
@ReadsAttributes({
        @ReadsAttribute(attribute = NuxeoAttributes.VAR_DOC_ID, description = "Document ID to use if the path isn't specified"),
        @ReadsAttribute(attribute = NuxeoAttributes.VAR_PATH, description = "Path to use, nx-docid overrides"),
        @ReadsAttribute(attribute = NuxeoAttributes.VAR_XPATH, description = "X-Path to use, defaults to file:content") })
@WritesAttributes({ @WritesAttribute(attribute = NuxeoAttributes.VAR_DOC_ID, description = "Added if not present"),
        @WritesAttribute(attribute = NuxeoAttributes.VAR_FILENAME, description = "Filename of the blob"),
        @WritesAttribute(attribute = NuxeoAttributes.VAR_ERROR, description = "Error set if problem occurs") })
@InputRequirement(Requirement.INPUT_REQUIRED)
public class GetNuxeoBlob extends AbstractNuxeoProcessor {

    public static final PropertyDescriptor XPATH = new PropertyDescriptor.Builder().name("XPATH")
                                                                                   .displayName("Property X-Path")
                                                                                   .description(
                                                                                           "Document x-path property to retrieve. {nx-xpath}")
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
        descriptors.add(XPATH);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        // Get target blob
        String xpath = getArg(context, flowFile, VAR_XPATH, XPATH);
        String docId = getArg(context, flowFile, VAR_DOC_ID, null);
        String path = getArg(context, flowFile, VAR_PATH, DOC_PATH);

        if (StringUtils.isBlank(docId) && StringUtils.isBlank(path)) {
            return;
        }

        // Create success path
        FlowFile blobFile = session.create(flowFile);
        try {
            // Invoke document operation
            Repository rep = getRepository(context, flowFile);
            StreamBlob blob = docId != null ? rep.streamBlobById(docId, xpath) : rep.streamBlobByPath(path, xpath);
            session.putAttribute(blobFile, VAR_XPATH, xpath);
            session.putAttribute(blobFile, VAR_FILENAME, blob.getFilename());
            session.putAttribute(blobFile, "mime.type", blob.getMimeType());

            // Write to flowfile
            try (InputStream in = blob.getStream(); OutputStream out = session.write(blobFile)) {
                IOUtils.copy(in, out);
            } catch (IOException e) {
                session.putAttribute(flowFile, VAR_ERROR, e.getMessage());
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            session.transfer(blobFile, REL_SUCCESS);
        } catch (NuxeoClientException nce) {
            session.remove(blobFile);
            getLogger().error("Unable to retrieve blob", nce);
            session.putAttribute(flowFile, VAR_ERROR, String.valueOf(nce));
            session.transfer(flowFile, REL_FAILURE);
        } finally {
            session.transfer(flowFile, REL_ORIGINAL);
        }
    }
}
