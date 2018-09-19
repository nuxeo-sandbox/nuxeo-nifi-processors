package org.nuxeo.client.util;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.nuxeo.client.objects.Document;

public class DocumentPathTest {

    private Document doc;

    private Object[] files;

    @Before
    public void createDocument() {
        this.doc = Document.createWithName("test.doc", "File");
        Map<String, Object> properties = new HashMap<>();
        properties.put("sample:string", "test");
        properties.put("sample:bool", true);
        properties.put("sample:int", 42);
        properties.put("sample:float", 2.7f);
        properties.put("sample:double", Math.PI);

        Object[] array = new Object[] { "one", 2, Pair.of("three", 3), new Object[] { 4, "four", 4.0f, 4.0d } };
        properties.put("sample:array", array);

        this.files = new Object[] { new MockFile("a", "text/plain", "abc", 123),
                new MockFile("b", "text/html", "def", 456), new MockFile("c", "application/pdf", "ghi", 789),
                new MockFile("d", "application/octet-stream", "jkl", 101112),
                new MockFile("e", "video/mp4", "mno", 131415) };
        properties.put("files:files", this.files);
        this.doc.setProperties(properties);
    }

    private void test(String path, Object expected) {
        Object val = DocumentPath.evaluate(this.doc, path);
        if (expected instanceof Object[] && val instanceof Object[]) {
            Assert.assertArrayEquals((Object[]) expected, (Object[]) val);
        } else {
            Assert.assertEquals(expected, val);
        }
    }

    @Test
    public void testDocumentPath() {
        test("", null);
        test(" \t\n", null);
        test(".", this.doc);
        test("id", this.doc.getId());
        test("name", this.doc.getName());
        test("locked", this.doc.isLocked());
        test("state", this.doc.getState());
        test("invalid", null);
    }

    @Test
    public void testPropertiesPath() {
        test("sample:string", "test");
        test("sample:bool", true);
        test("sample:int", 42);
        test("sample:float", 2.7f);
        test("sample:double", Math.PI);
        test("properties/sample:int", 42);
        test("missing:value", null);
    }

    @Test
    public void testArrayPath() {
        test("sample:array/1", "one");
        test("sample:array/2", 2);
        test("sample:array/3/key", "three");
        test("sample:array/4", new Object[] { 4, "four", 4.0f, 4.0d });
        test("sample:array/4/2", "four");
    }

    @Test
    public void testFilters() {
        test("files:files", this.files);
        List<Object> expected = new LinkedList<>();
        expected.add(new MockFile("d", "application/octet-stream", "jkl", 101112));
        expected.add(new MockFile("e", "video/mp4", "mno", 131415));
        test("files:files/[length>1000]", expected);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidPaths() {
        test("properties/sample:int/test:int", 42);
        test("sample:int/nested:int", 42);
    }

    @Test(expected = NullPointerException.class)
    public void testMissingDoc() {
        DocumentPath.evaluate(null, "");
    }

    @Test(expected = NullPointerException.class)
    public void testMissingPath() {
        DocumentPath.evaluate(this.doc, null);
    }

}
