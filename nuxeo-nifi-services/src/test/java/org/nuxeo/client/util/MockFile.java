package org.nuxeo.client.util;

public class MockFile {

    private String name;

    private String mimeType;

    private String digest;

    private long length;

    public MockFile() {
        super();
    }

    public MockFile(String name, String mimeType, String digest, long length) {
        super();
        this.name = name;
        this.mimeType = mimeType;
        this.digest = digest;
        this.length = length;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMimeType() {
        return mimeType;
    }

    public void setMimeType(String mimeType) {
        this.mimeType = mimeType;
    }

    public String getDigest() {
        return digest;
    }

    public void setDigest(String digest) {
        this.digest = digest;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((digest == null) ? 0 : digest.hashCode());
        result = prime * result + (int) (length ^ (length >>> 32));
        result = prime * result + ((mimeType == null) ? 0 : mimeType.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MockFile other = (MockFile) obj;
        if (digest == null) {
            if (other.digest != null)
                return false;
        } else if (!digest.equals(other.digest))
            return false;
        if (length != other.length)
            return false;
        if (mimeType == null) {
            if (other.mimeType != null)
                return false;
        } else if (!mimeType.equals(other.mimeType))
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        return true;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("MockFile [name=")
               .append(name)
               .append(", mimeType=")
               .append(mimeType)
               .append(", digest=")
               .append(digest)
               .append(", length=")
               .append(length)
               .append("]");
        return builder.toString();
    }

}
