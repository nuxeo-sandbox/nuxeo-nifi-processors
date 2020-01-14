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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import org.nuxeo.client.objects.blob.StreamBlob;
import org.nuxeo.client.objects.upload.BatchUpload;
import org.nuxeo.client.objects.upload.BatchUploadManager;
import org.nuxeo.client.spi.NuxeoClientException;

@Tags({ "nuxeo", "upload", "put", "blob" })
@CapabilityDescription("Upload blob data to Nuxeo.")
@SeeAlso({ NuxeoBlobOperation.class, GetNuxeoBlob.class })
@ReadsAttributes({ @ReadsAttribute(attribute = NuxeoAttributes.VAR_FILENAME, description = "Blob filename") })
@WritesAttributes({ @WritesAttribute(attribute = NuxeoAttributes.VAR_BATCH, description = "Upload batch ID"),
        @WritesAttribute(attribute = NuxeoAttributes.VAR_INDEX, description = "Batch index"),
        @WritesAttribute(attribute = NuxeoAttributes.VAR_FILENAME, description = "Blob filename"),
        @WritesAttribute(attribute = NuxeoAttributes.VAR_ERROR, description = "Error set if problem occurs") })
public class UploadNuxeoBlob extends AbstractNuxeoProcessor {

    public static final PropertyDescriptor FILE_NAME = new PropertyDescriptor.Builder().name("FILE_NAME")
                                                                                       .displayName("File Name")
                                                                                       .description("File Name to use.")
                                                                                       .expressionLanguageSupported(
                                                                                               ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                                                                                       .required(false)
                                                                                       .addValidator(
                                                                                               StandardValidators.NON_BLANK_VALIDATOR)
                                                                                       .build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(NUXEO_CLIENT_SERVICE);
        descriptors.add(FILE_NAME);
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
        String filename = getArg(context, flowFile, VAR_FILENAME, FILE_NAME);
        if (filename == null) {
            filename = "data-" + flowFile.getId();
        }

        // Create success path
        FlowFile blobFile = session.create(flowFile);
        try {
            // Invoke document operation
            BatchUploadManager upload = this.nuxeoClient.batchUploadManager();
            BatchUpload batch = upload.createBatch();

            // Write to repository
            try (InputStream in = session.read(flowFile)) {
                StreamBlob stream = new StreamBlob(in, filename);
                batch.upload("0", stream);
            } catch (IOException e) {
                throw new NuxeoClientException(e.getMessage(), e);
            }
            if (filename != null) {
                session.putAttribute(blobFile, VAR_FILENAME, filename);
            }

            session.putAttribute(blobFile, VAR_BATCH, batch.getBatchId());
            session.putAttribute(blobFile, VAR_INDEX, "0");
            session.transfer(blobFile, REL_SUCCESS);
        } catch (NuxeoClientException nce) {
            session.remove(blobFile);

            getLogger().error("Unable to upload blob", nce);
            session.putAttribute(flowFile, VAR_ERROR, String.valueOf(nce));
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        session.transfer(flowFile, REL_ORIGINAL);
    }
}
