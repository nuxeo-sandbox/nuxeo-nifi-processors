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
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.nuxeo.client.objects.Document;
import org.nuxeo.client.objects.blob.Blob;
import org.nuxeo.client.objects.blob.StreamBlob;
import org.nuxeo.client.spi.NuxeoClientException;
import org.nuxeo.client.spi.NuxeoClientRemoteException;

@Tags({ "nuxeo", "put", "blob" })
@CapabilityDescription("Invoke the Nuxeo Random Importer function")
@SeeAlso({ GetNuxeoBlob.class })
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class PutNuxeoBlob extends AbstractNuxeoProcessor {

	private List<PropertyDescriptor> descriptors;

	private Set<Relationship> relationships;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(NUXEO_CLIENT_SERVICE);
		descriptors.add(TARGET_REPO);
		descriptors.add(TARGET_PATH);
		descriptors.add(TARGET_NAME);
		descriptors.add(TARGET_TYPE);
		descriptors.add(TARGET_TITLE);
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_FAILURE);
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
			return;
		}

		// Evaluate target path
		String path = getArg(context, flowFile, "nx-path", TARGET_PATH);
		final String name = getArg(context, flowFile, "nx-name", TARGET_NAME);
		String type = getArg(context, flowFile, "nx-type", TARGET_TYPE);
		String title = getArg(context, flowFile, "nx-title", TARGET_TITLE);
		if (title == null) {
			title = name;
		}

		try {

			getLogger().info("Creating document blob with name: " + name + " and type: " + type + " at path: " + path);
			Document doc = Document.createWithName(name, type);
			doc.setPropertyValue("dc:title", title);
			doc = getRepository(context).createDocumentByPath(path, doc);

			try (InputStream inStream = session.read(flowFile)) {
				Blob inBlob = new StreamBlob(inStream, name);
				nuxeoClient.operation("Blob.AttachOnDocument").param("document", doc.getPath()).input(inBlob).execute();
			} catch (IOException iox) {
				getLogger().error("Unable to store document blob", iox);
				session.putAttribute(flowFile, "nx-error", String.valueOf(iox));
				session.transfer(flowFile, REL_FAILURE);
				return;
			}

			session.putAttribute(flowFile, "nx-docid", doc.getId());
			session.transfer(flowFile, REL_SUCCESS);
		} catch (NuxeoClientRemoteException ncre) {
			getLogger().error("Remote error: (" + ncre.getStatus() + ")\n" + ncre.getErrorBody(), ncre);
			session.putAttribute(flowFile, "nx-status", String.valueOf(ncre.getStatus()));
			session.putAttribute(flowFile, "nx-error", ncre.getErrorBody());
			session.transfer(flowFile, REL_FAILURE);
		} catch (NuxeoClientException nce) {
			getLogger().error("Unable to store document blob", nce);
			session.putAttribute(flowFile, "nx-error", String.valueOf(nce));
			session.transfer(flowFile, REL_FAILURE);
		}
	}

}
