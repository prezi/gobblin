/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package org.apache.gobblin.http;

import org.apache.avro.specific.SpecificData;

@SuppressWarnings("all")
/** Represents an http output record */
@org.apache.avro.specific.AvroGenerated
public class HttpRequestResponseRecord extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -6777339196947958577L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"HttpRequestResponseRecord\",\"namespace\":\"org.apache.gobblin.http\",\"doc\":\"Represents an http output record\",\"fields\":[{\"name\":\"requestUrl\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"method\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"contentType\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}]},{\"name\":\"statusCode\",\"type\":\"int\"},{\"name\":\"body\",\"type\":[\"null\",\"bytes\"]}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.String requestUrl;
  @Deprecated public java.lang.String method;
  @Deprecated public java.lang.String contentType;
  @Deprecated public int statusCode;
  @Deprecated public java.nio.ByteBuffer body;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public HttpRequestResponseRecord() {}

  /**
   * All-args constructor.
   * @param requestUrl The new value for requestUrl
   * @param method The new value for method
   * @param contentType The new value for contentType
   * @param statusCode The new value for statusCode
   * @param body The new value for body
   */
  public HttpRequestResponseRecord(java.lang.String requestUrl, java.lang.String method, java.lang.String contentType, java.lang.Integer statusCode, java.nio.ByteBuffer body) {
    this.requestUrl = requestUrl;
    this.method = method;
    this.contentType = contentType;
    this.statusCode = statusCode;
    this.body = body;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return requestUrl;
    case 1: return method;
    case 2: return contentType;
    case 3: return statusCode;
    case 4: return body;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: requestUrl = (java.lang.String)value$; break;
    case 1: method = (java.lang.String)value$; break;
    case 2: contentType = (java.lang.String)value$; break;
    case 3: statusCode = (java.lang.Integer)value$; break;
    case 4: body = (java.nio.ByteBuffer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'requestUrl' field.
   * @return The value of the 'requestUrl' field.
   */
  public java.lang.String getRequestUrl() {
    return requestUrl;
  }

  /**
   * Sets the value of the 'requestUrl' field.
   * @param value the value to set.
   */
  public void setRequestUrl(java.lang.String value) {
    this.requestUrl = value;
  }

  /**
   * Gets the value of the 'method' field.
   * @return The value of the 'method' field.
   */
  public java.lang.String getMethod() {
    return method;
  }

  /**
   * Sets the value of the 'method' field.
   * @param value the value to set.
   */
  public void setMethod(java.lang.String value) {
    this.method = value;
  }

  /**
   * Gets the value of the 'contentType' field.
   * @return The value of the 'contentType' field.
   */
  public java.lang.String getContentType() {
    return contentType;
  }

  /**
   * Sets the value of the 'contentType' field.
   * @param value the value to set.
   */
  public void setContentType(java.lang.String value) {
    this.contentType = value;
  }

  /**
   * Gets the value of the 'statusCode' field.
   * @return The value of the 'statusCode' field.
   */
  public java.lang.Integer getStatusCode() {
    return statusCode;
  }

  /**
   * Sets the value of the 'statusCode' field.
   * @param value the value to set.
   */
  public void setStatusCode(java.lang.Integer value) {
    this.statusCode = value;
  }

  /**
   * Gets the value of the 'body' field.
   * @return The value of the 'body' field.
   */
  public java.nio.ByteBuffer getBody() {
    return body;
  }

  /**
   * Sets the value of the 'body' field.
   * @param value the value to set.
   */
  public void setBody(java.nio.ByteBuffer value) {
    this.body = value;
  }

  /**
   * Creates a new HttpRequestResponseRecord RecordBuilder.
   * @return A new HttpRequestResponseRecord RecordBuilder
   */
  public static org.apache.gobblin.http.HttpRequestResponseRecord.Builder newBuilder() {
    return new org.apache.gobblin.http.HttpRequestResponseRecord.Builder();
  }

  /**
   * Creates a new HttpRequestResponseRecord RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new HttpRequestResponseRecord RecordBuilder
   */
  public static org.apache.gobblin.http.HttpRequestResponseRecord.Builder newBuilder(org.apache.gobblin.http.HttpRequestResponseRecord.Builder other) {
    return new org.apache.gobblin.http.HttpRequestResponseRecord.Builder(other);
  }

  /**
   * Creates a new HttpRequestResponseRecord RecordBuilder by copying an existing HttpRequestResponseRecord instance.
   * @param other The existing instance to copy.
   * @return A new HttpRequestResponseRecord RecordBuilder
   */
  public static org.apache.gobblin.http.HttpRequestResponseRecord.Builder newBuilder(org.apache.gobblin.http.HttpRequestResponseRecord other) {
    return new org.apache.gobblin.http.HttpRequestResponseRecord.Builder(other);
  }

  /**
   * RecordBuilder for HttpRequestResponseRecord instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<HttpRequestResponseRecord>
    implements org.apache.avro.data.RecordBuilder<HttpRequestResponseRecord> {

    private java.lang.String requestUrl;
    private java.lang.String method;
    private java.lang.String contentType;
    private int statusCode;
    private java.nio.ByteBuffer body;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(org.apache.gobblin.http.HttpRequestResponseRecord.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.requestUrl)) {
        this.requestUrl = data().deepCopy(fields()[0].schema(), other.requestUrl);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.method)) {
        this.method = data().deepCopy(fields()[1].schema(), other.method);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.contentType)) {
        this.contentType = data().deepCopy(fields()[2].schema(), other.contentType);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.statusCode)) {
        this.statusCode = data().deepCopy(fields()[3].schema(), other.statusCode);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.body)) {
        this.body = data().deepCopy(fields()[4].schema(), other.body);
        fieldSetFlags()[4] = true;
      }
    }

    /**
     * Creates a Builder by copying an existing HttpRequestResponseRecord instance
     * @param other The existing instance to copy.
     */
    private Builder(org.apache.gobblin.http.HttpRequestResponseRecord other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.requestUrl)) {
        this.requestUrl = data().deepCopy(fields()[0].schema(), other.requestUrl);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.method)) {
        this.method = data().deepCopy(fields()[1].schema(), other.method);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.contentType)) {
        this.contentType = data().deepCopy(fields()[2].schema(), other.contentType);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.statusCode)) {
        this.statusCode = data().deepCopy(fields()[3].schema(), other.statusCode);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.body)) {
        this.body = data().deepCopy(fields()[4].schema(), other.body);
        fieldSetFlags()[4] = true;
      }
    }

    /**
      * Gets the value of the 'requestUrl' field.
      * @return The value.
      */
    public java.lang.String getRequestUrl() {
      return requestUrl;
    }

    /**
      * Sets the value of the 'requestUrl' field.
      * @param value The value of 'requestUrl'.
      * @return This builder.
      */
    public org.apache.gobblin.http.HttpRequestResponseRecord.Builder setRequestUrl(java.lang.String value) {
      validate(fields()[0], value);
      this.requestUrl = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'requestUrl' field has been set.
      * @return True if the 'requestUrl' field has been set, false otherwise.
      */
    public boolean hasRequestUrl() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'requestUrl' field.
      * @return This builder.
      */
    public org.apache.gobblin.http.HttpRequestResponseRecord.Builder clearRequestUrl() {
      requestUrl = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'method' field.
      * @return The value.
      */
    public java.lang.String getMethod() {
      return method;
    }

    /**
      * Sets the value of the 'method' field.
      * @param value The value of 'method'.
      * @return This builder.
      */
    public org.apache.gobblin.http.HttpRequestResponseRecord.Builder setMethod(java.lang.String value) {
      validate(fields()[1], value);
      this.method = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'method' field has been set.
      * @return True if the 'method' field has been set, false otherwise.
      */
    public boolean hasMethod() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'method' field.
      * @return This builder.
      */
    public org.apache.gobblin.http.HttpRequestResponseRecord.Builder clearMethod() {
      method = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'contentType' field.
      * @return The value.
      */
    public java.lang.String getContentType() {
      return contentType;
    }

    /**
      * Sets the value of the 'contentType' field.
      * @param value The value of 'contentType'.
      * @return This builder.
      */
    public org.apache.gobblin.http.HttpRequestResponseRecord.Builder setContentType(java.lang.String value) {
      validate(fields()[2], value);
      this.contentType = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'contentType' field has been set.
      * @return True if the 'contentType' field has been set, false otherwise.
      */
    public boolean hasContentType() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'contentType' field.
      * @return This builder.
      */
    public org.apache.gobblin.http.HttpRequestResponseRecord.Builder clearContentType() {
      contentType = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'statusCode' field.
      * @return The value.
      */
    public java.lang.Integer getStatusCode() {
      return statusCode;
    }

    /**
      * Sets the value of the 'statusCode' field.
      * @param value The value of 'statusCode'.
      * @return This builder.
      */
    public org.apache.gobblin.http.HttpRequestResponseRecord.Builder setStatusCode(int value) {
      validate(fields()[3], value);
      this.statusCode = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'statusCode' field has been set.
      * @return True if the 'statusCode' field has been set, false otherwise.
      */
    public boolean hasStatusCode() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'statusCode' field.
      * @return This builder.
      */
    public org.apache.gobblin.http.HttpRequestResponseRecord.Builder clearStatusCode() {
      fieldSetFlags()[3] = false;
      return this;
    }

    /**
      * Gets the value of the 'body' field.
      * @return The value.
      */
    public java.nio.ByteBuffer getBody() {
      return body;
    }

    /**
      * Sets the value of the 'body' field.
      * @param value The value of 'body'.
      * @return This builder.
      */
    public org.apache.gobblin.http.HttpRequestResponseRecord.Builder setBody(java.nio.ByteBuffer value) {
      validate(fields()[4], value);
      this.body = value;
      fieldSetFlags()[4] = true;
      return this;
    }

    /**
      * Checks whether the 'body' field has been set.
      * @return True if the 'body' field has been set, false otherwise.
      */
    public boolean hasBody() {
      return fieldSetFlags()[4];
    }


    /**
      * Clears the value of the 'body' field.
      * @return This builder.
      */
    public org.apache.gobblin.http.HttpRequestResponseRecord.Builder clearBody() {
      body = null;
      fieldSetFlags()[4] = false;
      return this;
    }

    @Override
    public HttpRequestResponseRecord build() {
      try {
        HttpRequestResponseRecord record = new HttpRequestResponseRecord();
        record.requestUrl = fieldSetFlags()[0] ? this.requestUrl : (java.lang.String) defaultValue(fields()[0]);
        record.method = fieldSetFlags()[1] ? this.method : (java.lang.String) defaultValue(fields()[1]);
        record.contentType = fieldSetFlags()[2] ? this.contentType : (java.lang.String) defaultValue(fields()[2]);
        record.statusCode = fieldSetFlags()[3] ? this.statusCode : (java.lang.Integer) defaultValue(fields()[3]);
        record.body = fieldSetFlags()[4] ? this.body : (java.nio.ByteBuffer) defaultValue(fields()[4]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  private static final org.apache.avro.io.DatumWriter
    WRITER$ = new org.apache.avro.specific.SpecificDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  private static final org.apache.avro.io.DatumReader
    READER$ = new org.apache.avro.specific.SpecificDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}
