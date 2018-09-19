package org.nuxeo.client.util;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.jxpath.JXPathContext;
import org.apache.commons.jxpath.Pointer;
import org.nuxeo.client.objects.Document;

public class DocumentPath {

    public static final <T> T evaluate(Document doc, String xpath) {
        DocumentPath path = new DocumentPath(doc);
        return path.evaluate(xpath);
    }

    private final Document doc;

    public DocumentPath(Document doc) {
        super();
        this.doc = doc;
        if (this.doc == null) {
            throw new NullPointerException("missing doc");
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T evaluate(String xpath) {
        if (xpath == null) {
            throw new NullPointerException("missing xpath");
        }
        if ("".equals(xpath.trim())) {
            return null;
        }
        Object ref = this.doc;
        try (Scanner scan = new Scanner(xpath)) {
            scan.useDelimiter("/");
            while (ref != null && scan.hasNext()) {
                String part = scan.next();

                if (part.matches("[a-zA-Z_:][\\w\\-:]*:[a-zA-Z_:][\\w\\-:]*")) {
                    if (ref != this.doc && ref != this.doc.getProperties()) {
                        throw new IllegalArgumentException("Path references schema in non-referencable context.");
                    }
                    ref = this.doc.getPropertyValue(part);
                } else {
                    ref = resolve(ref, part);
                }
            }
        }
        return (T) ref;
    }

    private Object resolve(Object ref, String part) {

        // Handle array pathing and filtering
        if (ref instanceof Object[]) {
            int idx = -1;
            int len = ((Object[]) ref).length;
            try {
                idx = Integer.parseInt(part);
                if (idx < 0) {
                    idx = len + idx;
                }
                if (idx < 0 || idx > len) {
                    throw new IllegalArgumentException("Array index out of range");
                }
                part = "$root[" + idx + "]";
            } catch (NumberFormatException nfe) {
                // Fall through to test array based selections
                part = "$root" + part;
            }
        }

        // Lazily create list by iterating through pointers
        JXPathContext jx = JXPathContext.newContext(ref);
        jx.getVariables().declareVariable("root", ref);

        @SuppressWarnings("unchecked")
        Iterator<Pointer> ptr = jx.iteratePointers(part);
        List<Object> list = null;
        Object val = null;
        while (ptr.hasNext()) {
            Pointer p = ptr.next();
            if (val != null && list == null) {
                list = new LinkedList<>();
                list.add(val);
            }
            val = p.getValue();
            if (list != null) {
                list.add(val);
            }
        }
        if (list != null) {
            return list;
        }
        return val;
    }

}
