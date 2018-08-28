package org.nuxeo.labs.nifi.processors;

import static org.nuxeo.client.Operations.BLOB_ATTACH_ON_DOCUMENT;
import static org.nuxeo.client.Operations.ES_WAIT_FOR_INDEXING;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.nuxeo.client.NuxeoClient;
import org.nuxeo.client.objects.Document;
import org.nuxeo.client.objects.blob.FileBlob;
import org.nuxeo.client.spi.auth.BasicAuthInterceptor;
import org.nuxeo.common.utils.FileUtils;
import org.nuxeo.labs.nifi.services.NuxeoClientServiceImpl;

import okhttp3.Interceptor;

public abstract class BaseTest {

    public static final String BASE_URL = "http://localhost:8080/nuxeo";

    public static final String LOGIN = "Administrator";

    public static final String PASSWORD = "Administrator";

    static final String REST_API_URL = "http://localhost:8080/nuxeo";

    public static final String FOLDER_2_FILE = "/folder_2/file";

    protected final NuxeoClient nuxeoClient = createClient().schemas("*");

    public BaseTest() {
        super();
    }

    public static void addController(final TestRunner testRunner) throws InitializationException {
        Map<String, String> props = new HashMap<>();
        props.put("SERVER_URL", REST_API_URL);
        props.put("AUTH_TYPE", "Basic");
        props.put("USERNAME", "Administrator");
        props.put("CREDENTIALS", "Administrator");

        NuxeoClientServiceImpl controller = new NuxeoClientServiceImpl();
        testRunner.addControllerService("localhost", controller, props);
        testRunner.enableControllerService(controller);
        testRunner.assertValid(controller);
    }

    /**
     * @return A {@link NuxeoClient} filled with Nuxeo Server URL and default basic authentication.
     */
    public static NuxeoClient createClient() {
        return createClient(LOGIN, PASSWORD);
    }

    /**
     * @return A {@link NuxeoClient} filled with Nuxeo Server URL and input basic authentication.
     */
    public static NuxeoClient createClient(String login, String password) {
        return createClientBuilder(login, password).connect();
    }

    /**
     * @return A {@link NuxeoClient.Builder} filled with Nuxeo Server URL and input basic authentication.
     */
    public static NuxeoClient.Builder createClientBuilder(String login, String password) {
        return createClientBuilder(new BasicAuthInterceptor(login, password));
    }

    /**
     * @return A {@link NuxeoClient.Builder} filled with Nuxeo Server URL and given authentication.
     */
    protected static NuxeoClient.Builder createClientBuilder(Interceptor authenticationMethod) {
        return new NuxeoClient.Builder().url(BASE_URL).authentication(authenticationMethod).timeout(60);
    }

    public void initDocuments() {
        // Create documents
        for (int i = 1; i < 3; i++) {
            Document doc = Document.createWithName("folder_" + i, "Folder");
            doc.setPropertyValue("dc:title", "Folder " + i);
            nuxeoClient.repository().createDocumentByPath("/", doc);
        }

        for (int i = 0; i < 3; i++) {
            Document doc = Document.createWithName("note_" + i, "Note");
            doc.setPropertyValue("dc:title", "Note " + i);
            doc.setPropertyValue("note:note", "Note " + i);
            nuxeoClient.repository().createDocumentByPath("/folder_1", doc);
        }

        // Create a file
        Document doc = Document.createWithName("file", "File");
        doc.setPropertyValue("dc:title", "File");
        nuxeoClient.repository().createDocumentByPath("/folder_2", doc);
        // Attach a light blob
        File file = FileUtils.getResourceFileFromContext("blob.json");
        FileBlob fileBlob = new FileBlob(file, "blob.json", "text/plain");
        nuxeoClient.operation(BLOB_ATTACH_ON_DOCUMENT)
                   .voidOperation(true)
                   .param("document", FOLDER_2_FILE)
                   .input(fileBlob)
                   .execute();
        nuxeoClient.operation("Document.AddToFavorites")
                   // .voidOperation(true)
                   .input(FOLDER_2_FILE)
                   .execute();
        // page providers can leverage Elasticsearch so wait for indexing before starting tests
        nuxeoClient.operation(ES_WAIT_FOR_INDEXING).param("refresh", true).param("waitForAudit", true).execute();
    }

}
