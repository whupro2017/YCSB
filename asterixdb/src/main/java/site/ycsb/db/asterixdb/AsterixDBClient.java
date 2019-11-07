/**
 * Copyright (c) 2017 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package site.ycsb.db.asterixdb;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
//import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.json.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map.Entry;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.Iterator;
import java.util.HashSet;
import java.util.Random;
import java.util.logging.Logger;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.measurements.Measurements;

/**
 * A class that wraps the AsterixDBClient to allow it to be interfaced with YCSB.
 * This class extends {@link DB} and implements the database interface used by YCSB client.
 */
public class AsterixDBClient extends DB {
  protected static final Logger LOGGER = Logger.getLogger(AsterixDBClient.class.getName());

  // BFC: Change to fix broken build (with HBase 0.20.6)
  //private static final Configuration config = HBaseConfiguration.create();
  // Cannot reuse the method, just reuse it by threads pool in the future.
  //	public static int QUERY_CONNECT_COUNT = 4;
  //	public static int MODIFY_CONNECT_COUNT = 4;
  private abstract class OperateCache {
    public String _tableName;
    private String header;
    protected String body;
    private String tailer;
    //		private String tuples;
    protected int cached = 0;
    protected static final int DEFAULT_THRESHOLD = 100;
    private List<Entry<String, HashMap<String, ByteIterator>>> data =
        new ArrayList<Entry<String, HashMap<String, ByteIterator>>>();

    //		private HashMap<String,ByteIterator> values;
    public OperateCache() {
      body = "";
    }

    public void setHeader(String head) {
      header = head;
    }

		/*public String getHeader() {
			return this.header;
		}*/

    public void setTailer(String tail) {
      tailer = tail;
    }

		/*public String getTail() {
			return this.tailer;
		}*/

    public String getSQL() {
      return header + body + tailer;
    }

    public Status addOperator(String rkey, HashMap<String, ByteIterator> rvalues) {
      return Status.OK;
    }

    public Status cleanUp() {
      Status ret = Status.OK;
      if (cached > 0)
        ret = insertDelete(getSQL());
      cached = 0;
      body = "";
      return ret;
    }
  }

  private class InsertCache extends OperateCache {
    public InsertCache() {
      super();
      this.setHeader(use_dataverse + "insert into dataset " + tableName + "( for $l in [");
      this.setTailer("] return $l );");
    }

    public InsertCache(String tableName) {
      super();
      this._tableName = tableName;
      this.setHeader(use_dataverse + "insert into dataset " + _tableName + "( for $l in [");
      this.setTailer("] return $l );");
    }

    @Override public Status addOperator(String key, HashMap<String, ByteIterator> tuple) {
      super.addOperator(key, tuple);
      {
        if (cached++ > 0)
          body += ",";
        body += "{\"" + AsterixDBConstant.PRIMARY_KEY + "\":\"" + key + "\"";
        Set<Entry<String, ByteIterator>> kvs = null;
        if (null != tuple && !tuple.isEmpty()) {
          kvs = tuple.entrySet();
          Iterator<Entry<String, ByteIterator>> its = kvs.iterator();
          while (its.hasNext()) {
            Entry<String, ByteIterator> it = its.next();
            body += ",\"" + it.getKey() + "\":\"" + it.getValue().toString() + "\"";
          }
        }
        body += "}";
      }
      if (cached == DEFAULT_THRESHOLD) {
        Status ret = insertDelete(getSQL());
        cached = 0;
        body = "";
        return ret;
      } else
        return Status.OK;
    }
  }

  private class DeleteCache extends OperateCache {
    public DeleteCache() {
      super();
      this.setHeader(use_dataverse + "delete $t from dataset " + tableName + "where ");
      this.setTailer(" ;");
    }

    public DeleteCache(String tableName) {
      super();
      this._tableName = tableName;
      this.setHeader(use_dataverse + "delete $t from dataset " + _tableName + " where ");
      this.setTailer(" ;");
    }

    @Override public Status addOperator(String rkey, HashMap<String, ByteIterator> rvalues) {
      super.addOperator(rkey, rvalues);
      {
        if (cached++ > 0)
          body += " or ";
        body += "$t." + AsterixDBConstant.PRIMARY_KEY + " = \"" + rkey + "\"";
      }
      if (cached == DEFAULT_THRESHOLD) {
        Status ret = insertDelete(getSQL());
        cached = 0;
        body = "";
        return ret;
      } else
        return Status.OK;
    }
  }

  private boolean _batching = false;

  public boolean getBatch() {
    return _batching;
  }

  public void setBatch(boolean batch) {
    _batching = batch;
  }

  public Status cleanUp() {
    Status ret = Status.OK;
    if (_batching) {
      Iterator<Entry<String, InsertCache>> eits = insertCache.entrySet().iterator();
      while (eits.hasNext()) {
        Entry<String, InsertCache> eit = eits.next();
        //ret |= eit.getValue().cleanUp();
        if (!eit.getValue().cleanUp().equals(Status.OK))
          ret = Status.ERROR;
      }

      Iterator<Entry<String, DeleteCache>> edts = deleteCache.entrySet().iterator();
      while (edts.hasNext()) {
        Entry<String, DeleteCache> edt = edts.next();
        if (!edt.getValue().cleanUp().equals(Status.OK))
          ret = Status.ERROR;
      }
    }
    return ret;
  }

  private HashMap<String, InsertCache> insertCache = null;//new HashMap<String, InsertCache>(); //InsertCache();
  private HashMap<String, DeleteCache> deleteCache = null;//new HashMap<String, InsertCache>();//DeleteCache();
  public static String head = "Iwiperupweiruwepiourpioewurpwoeiurpiweurpouwerupweriuwepiorewpruewpriuewpriuewpr"
      + "weipurpweiorupewoiurpewiorupweirupewiourpewiourpweiorupewiruwepiruwperiuweproiupiweorupw"
      + "piouweirouwepiruwepioruwepioruwepiourwepoirupweiorupewiorupewoirupweiourpweioruewpiruweprui"
      + "ipweurpweiurpewiurpwqieurpiurpiowuqerpqruwepuqrewpqirsiup324u28394p234j;3k4jp32i4u234pi234;324jkp"
      + "2p34702934upp2i34up234u2i-++(=3294u2p34j234pusjkewp4j3iu4wp3jw42p3u4j324234iu23pj324uiuo23"
      + "892347p23i423up4j23k423u4p23oi4234u234";
  private static int DEFAULT_LOCAL_COUNT = 1000;
  public static String BASIC_URL = "http://127.0.0.1:19002";
  //	public static String BASIC_URL = "http://10.10.10.73:19002";
  public static String DEFAULT_TABLE = "usertable";
  public static String DEFAULT_TYPE = "YCSBAsterType";
  public String tableName = null;
  public static String use_dataverse = "use dataverse ";
  public static String create_table_adm =
      "drop dataset " + DEFAULT_TABLE + " if exists;" + "drop type " + DEFAULT_TYPE + " if exists; "
          + "create type YCSBAsterType as open {YCSB_KEY: string, " + "FIELD1: string?,"
          + "FIELD2: string?, FIELD3: string?, FIELD4: string?, FIELD5: string?, FIELD6: string?, FIELD7: string?, "
          + "FIELD8: string?, FIELD9: string?, FIELD10: string?};" + "drop dataset " + DEFAULT_TABLE + " if exists; "
          + "create dataset " + DEFAULT_TABLE + "(" + DEFAULT_TYPE + ")" + " primary key YCSB_KEY";
  private Set<String> default_fields;

  public enum OutputFormat {
    NONE("", ""), ADM("", "application/adm"), JSON("", "application/json");

    private final String extension;
    private final String mimetype;

    OutputFormat(String ext, String mime) {
      this.extension = ext;
      this.mimetype = mime;
    }

    public String extension() {
      return extension;
    }

    public String mimeType() {
      return mimetype;
    }
  }

  ;

  public boolean _debug = false;

  private String _dataVerse = "";
  //	private String _dataSet = "";
  private static HttpClient client = new HttpClient();
  // for async query
  private static String result_end = "/query/result";
  private static String query_end = "/query";
  private static String update_end = "/update";
  private static String schema_end = "/ddl";
  //	private List<GetMethod> queries = null;
  //	private List<PostMethod> modifies = null;
  //	private PostMethod schema = null;

  public static final int Ok = 0;
  public static final int ServerError = -1;
  public static final int HttpError = -2;
  public static final int NoMatchingRecord = -3;

  public static final Object tableLock = new Object();

  private static InputStream executeHttpMethod(HttpMethod method) {
    //		HttpClient client = new HttpClient();
    try {
      int statusCodes = client.executeMethod(method);
      if (statusCodes != HttpStatus.SC_OK) {
        LOGGER.info("Method failed: " + method.getStatusCode());
      }
      return method.getResponseBodyAsStream();
    } catch (Exception e) {
      LOGGER.info(e.getMessage());
      e.printStackTrace();
    }
    return null;
  }

  //	private GetMethod queryMethod(int hashkey) {
  //		return queries.get(Math.abs(hashkey) % QUERY_CONNECT_COUNT);
  //	}
  //
  //	private PostMethod modifyMethod(int hashkey) {
  //		return modifies.get(Math.abs(hashkey) % MODIFY_CONNECT_COUNT);
  //	}

  private /*static*/ String[] handleError(HttpMethod method) throws Exception {
    String errorBody = method.getResponseBodyAsString();
    JSONObject result = new JSONObject(errorBody);
    if (_debug)
      System.out.println(errorBody);
    String[] errors =
        { result.getJSONArray("error-code").getString(0), result.getString("summary"), result.getString("stacktrace") };
    return errors;
  }

  public void create_schema() throws Exception {
    //		HttpClient client = new HttpClient();
    PostMethod schema = new PostMethod(BASIC_URL + schema_end);
    schema.setRequestEntity(new StringRequestEntity(use_dataverse + create_table_adm));
    schema.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
    int statusCode = HttpStatus.SC_OK;
    try {
      statusCode = client.executeMethod(schema);
    } catch (HttpException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    if (statusCode != HttpStatus.SC_OK) {
      LOGGER.info("Method failed: " + schema.getStatusCode());
      String[] errors = null;
      try {
        errors = handleError(schema);
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      LOGGER.info(errors[2]);
      throw new Exception(
          "DDL operation failed: " + errors[0] + "\nSUMMARY: " + errors[1] + "\nSTACKTRACE: " + errors[2]);
    }
  }

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void init() throws DBException {
    if ((getProperties().getProperty("debug") != null) && (getProperties().getProperty("debug").compareTo("true")
        == 0)) {
      _debug = true;
    }

    default_fields = new HashSet<String>();
    default_fields.add("FIELD1");
    default_fields.add("FIELD2");
    default_fields.add("FIELD3");
    default_fields.add("FIELD4");
    default_fields.add("FIELD5");
    default_fields.add("FIELD6");
    default_fields.add("FIELD7");
    default_fields.add("FIELD8");
    default_fields.add("FIELD9");
    default_fields.add("FIELD10");
    System.out.println(getProperties().getProperty("batch"));

    if (getProperties().getProperty("batch").compareTo("true") == 0) {
      _batching = true;
      insertCache = new HashMap<String, InsertCache>();
      deleteCache = new HashMap<String, DeleteCache>();
    }

    _dataVerse = getProperties().getProperty("dataverse");
    if (_dataVerse == null) {
      System.err.println("Error, must specify a columnfamily for HBase table");
      throw new DBException("No columnfamily specified");
    }
    use_dataverse += _dataVerse;
    use_dataverse += ";";

    //		queries = new ArrayList<GetMethod>();
    //		for (int i = 0; i < QUERY_CONNECT_COUNT; i++) {
    //			queries.add(new GetMethod(BASIC_URL + query_end));
    //		}
    //		modifies = new ArrayList<PostMethod>();
    //		for (int i = 0; i < MODIFY_CONNECT_COUNT; i++) {
    //			modifies.add(new PostMethod(BASIC_URL + update_end));
    //			modifies.get(i).getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
    //					new DefaultHttpMethodRetryHandler(3, false));
    //		}
    //		schema = new PostMethod(BASIC_URL + schema_end);
  }

  /**
   * Cleanup any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void cleanup() throws DBException {
    // Get the measurements instance as this is the only client that should
    // count clean up time like an update since autoflush is off.
    Measurements _measurements = Measurements.getMeasurements();
    //		try {
    long st = System.nanoTime();
    //			if (_hTable != null) {
    //				_hTable.flushCommits();
    //			}
    long en = System.nanoTime();
    _measurements.measure("UPDATE", (int) ((en - st) / 1000));
    //		} catch (IOException e) {
    //			throw new DBException(e);
    //		}
  }

  private void getResult(String sRet, Map<String, ByteIterator> hRet) {
    JSONParser parser = new JSONParser();
    Map<String, Object> rows = null;
    try {
      rows = (HashMap<String, Object>) parser.parse(sRet);
      int i = 0;
    } catch (ParseException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    if (null != rows) {
      Iterator<Entry<String, Object>> its = rows.entrySet().iterator();
      while (its.hasNext()) {
        Entry<String, Object> it = its.next();
        if (null != it.getValue())
          hRet.put(it.getKey(), new StringByteIterator(it.getValue().toString()));
        else
          hRet.put(it.getKey(), null);
      }
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  @Override public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    key.replace("\"", "\\\"");
    Set<String> nfields = new HashSet<String>();
    Iterator<String> nit = fields.iterator();
    while (nit.hasNext()) {
      nfields.add(nit.next().replace("\"", "\\\""));
    }
    fields = nfields;

    //if this is a "new" table, init HTable object.  Else, use existing one
    String str =
        use_dataverse + "for $l in dataset " + table + " where $l." + AsterixDBConstant.PRIMARY_KEY + " = \"" + key
            + "\" return ";
    if (null == fields || fields.size() == 0) {
      str += "$l;";
    } else {
      Iterator<String> itr = fields.iterator();
      str += "{";
      while (itr.hasNext()) {
        String field = itr.next();
        str += "\"" + field + "\":";
        str += "$l." + field;
        if (itr.hasNext())
          str += ",";
      }
      str += "}";
    }

    //		HttpMethod query = queryMethod(key.hashCode());
    GetMethod query = new GetMethod(BASIC_URL + query_end);
    query.setQueryString(new NameValuePair[] { new NameValuePair("query", str) });
    query.setRequestHeader("Accept", "application/json");
    query.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));

    InputStream sRet = executeHttpMethod(query);
    if (sRet == null)
      return Status.NOT_FOUND;
    try {
      JsonFactory jsonFactory = new JsonFactory();
      JsonParser resultParser = jsonFactory.createParser(sRet);
      while (resultParser.nextToken() == JsonToken.START_OBJECT) {
        while (resultParser.nextToken() != JsonToken.END_OBJECT) {
          String rkey = resultParser.getCurrentName();
          if (rkey.equals("results")) {
            resultParser.nextToken();
            while (resultParser.nextToken() != JsonToken.END_ARRAY) {
              String record = resultParser.getValueAsString();
              getResult(record, result);
							/*result.put(
									rkey,
									new ByteArrayByteIterator(record.getBytes()kv.getValue()));*/
              if (_debug) {
                System.out.println("Result for field: " + key + " is: " + record);
              }
            }
            //						break;
          } else if (rkey.equals("summary")) {
            String summary = resultParser.nextTextValue();
            throw new Exception("Could not find results by key in JSON Object, key: " + key + "summary: " + summary);
          }
        }
      }
    } catch (Exception e) {
      //			throw new Exception("Find asterix exception, key: " + key);
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table       The name of the table
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    startkey.replace("\"", "\\\"");
    Set<String> nfields = new HashSet<String>();
    Iterator<String> nit = fields.iterator();
    while (nit.hasNext()) {
      nfields.add(nit.next().replace("\"", "\\\""));
    }
    fields = nfields;

    //if this is a "new" table, init HTable object.  Else, use existing one
    String str = use_dataverse + "for $l in dataset " + table + " where $l." + AsterixDBConstant.PRIMARY_KEY + " >= \""
        + startkey + "\" limit " + recordcount + " return ";
    Iterator<String> itr = fields.iterator();
    while (itr.hasNext()) {
      String field = itr.next();
      str += "$l." + field;
    }

    //		HttpMethod query = queryMethod(startkey.hashCode());
    GetMethod query = new GetMethod(BASIC_URL + query_end);
    query.setQueryString(new NameValuePair[] { new NameValuePair("query", str) });
    query.setRequestHeader("Accept", "application/json");
    query.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));

    InputStream sRet = executeHttpMethod(query);
    if (sRet == null)
      return Status.NOT_FOUND;
    try {
      JsonFactory jsonFactory = new JsonFactory();
      JsonParser resultParser = jsonFactory.createParser(sRet);
      while (resultParser.nextToken() == JsonToken.START_OBJECT) {
        while (resultParser.nextToken() != JsonToken.END_OBJECT) {
          HashMap<String, ByteIterator> row = new HashMap<String, ByteIterator>();
          String rkey = resultParser.getCurrentName();
          if (rkey.equals("results")) {
            resultParser.nextToken();
            while (resultParser.nextToken() != JsonToken.END_ARRAY) {
              String record = resultParser.getValueAsString();
              //							HashMap<String, ByteIterator> hr = new HashMap<String, ByteIterator>();
              getResult(record, row);
							/*row.put(
									rkey,
									new ByteArrayByteIterator(record.getBytes()kv.getValue()));*/
              if (_debug) {
                System.out.println("Result for field: " + rkey + " is: " + record);
              }
            }
            result.add(row);
          } else if (rkey.equals("summary")) {
            String summary = resultParser.nextTextValue();
            throw new Exception(
                "Could not find results by key in JSON Object, key: " + startkey + "summary: " + summary);
          }
        }
      }
    } catch (Exception e) {
      //			throw new Exception("Find asterix exception, key: " + key);
      e.printStackTrace();
      return Status.ERROR;
    }

    return Status.OK;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
   * record key, overwriting any existing values with the same field name.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to write
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override public Status update(String table, String key, Map<String, ByteIterator> values) {
    key.replace("\"", "\\\"");
		/*Set<String>nfields = new HashSet<String>();
		Iterator<String>nit = fields.iterator();
		while (nit.hasNext()) {
			nfields.add(nit.next().replace("\"", "\\\""));
		}
		fields = nfields;*/
    HashMap<String, ByteIterator> nv = new HashMap<String, ByteIterator>();
    Iterator<Entry<String, ByteIterator>> iv = values.entrySet().iterator();
    while (iv.hasNext()) {
      Entry<String, ByteIterator> ev = iv.next();
      String fid = ev.getKey().replace("\"", "\\\"");
      String si = ev.getValue().toString().replace("\"", "\\\"");
      nv.put(fid, new StringByteIterator(si));
    }
    values = nv;
    if (_batching)
      return groupingUpdate(table, key, values);
    else {
      if (_debug) {
        System.out.println("****");
        Iterator<Entry<String, ByteIterator>> itr = values.entrySet().iterator();
        String outStr = new String();
        while (itr.hasNext()) {
          Entry<String, ByteIterator> it = itr.next();
          outStr += it.getKey() + ":" + it.getValue().toString() + ";";
        }
        System.out.println(outStr);
      }
      HashMap<String, ByteIterator> oldkv = new HashMap<String, ByteIterator>();
      Status ret = read(table, key, default_fields, oldkv);
      if (ret == Status.NOT_FOUND) {
        if (Status.OK != insert(table, key, values)) {
          try {
            throw new Exception("Insert failed: " + key);
          } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
          return Status.ERROR;
        } else
          return Status.OK;
      } else if (Status.OK != ret) {
        try {
          throw new Exception("Update_get failed: " + key);
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        return Status.ERROR;
      }
      // real update: delete and insert
      Iterator<Entry<String, ByteIterator>> its = values.entrySet().iterator();
      while (its.hasNext()) {
        Entry<String, ByteIterator> it = its.next();
        String keyr = it.getKey();
        ByteIterator valuer = it.getValue();
        oldkv.put(/*it.getKey(), it.getValue()*/keyr, valuer);
      }
      if (!delete(table, key).equals(Status.OK)) {
        try {
          throw new Exception("Update_del failed: " + key);
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        return Status.ERROR;
      }
      if (_debug) {
        System.out.println("****");
        Iterator<Entry<String, ByteIterator>> itr = oldkv.entrySet().iterator();
        String outStr = new String();
        while (itr.hasNext()) {
          Entry<String, ByteIterator> it = itr.next();
          outStr += it.getKey() + ":" + it.getValue().toString() + ";";
        }
        System.out.println(outStr);
      }
      if (!insert(table, key, oldkv).equals(Status.OK)) {
        try {
          throw new Exception("Update_ins failed: " + key);
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        return Status.ERROR;
      }

      if (_debug) {
        HashMap<String, ByteIterator> newkv = new HashMap<String, ByteIterator>();
        Status ret1 = read(table, key, default_fields, newkv);
        System.out.println("####");
        Iterator<Entry<String, ByteIterator>> itr = newkv.entrySet().iterator();
        String outStr = new String();
        while (itr.hasNext()) {
          Entry<String, ByteIterator> it = itr.next();
          outStr += it.getKey() + ":" + it.getValue().toString() + ";";
        }
        System.out.println(outStr);
      }

      return Status.OK;
    }
  }

  private int insertDeleteBatch(String str) {
    PostMethod modify = new PostMethod(BASIC_URL + update_end);
    modify.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
    modify.setRequestEntity(new StringRequestEntity(str));
    int statusCode = HttpStatus.SC_OK;
    return Ok;
  }

  private Status insertDelete(/*String key, */String str) {
    //		PostMethod modify = modifyMethod(key.hashCode());
    //		HttpClient client = new HttpClient();
    PostMethod modify = new PostMethod(BASIC_URL + update_end);
    modify.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
    modify.setRequestEntity(new StringRequestEntity(str));
    int statusCode = HttpStatus.SC_OK;
    try {
      statusCode = client.executeMethod(modify);
    } catch (HttpException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    if (statusCode != HttpStatus.SC_OK) {
      LOGGER.info(modify.getStatusCode() + ".\n" + str);
      String[] errors = null;
      try {
        errors = handleError(modify);
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      LOGGER.info(errors[2]);
      try {
        throw new Exception("Operator failed: " + errors[0] + "\nSUMMARY: " + errors[1] + "\nSTACKTRACE: " + errors[2]);
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      return Status.ERROR;
    }
    //		modify.removeRequestHeader(str);
    //		modify.releaseConnection();
    return Status.OK;
  }

  private Status groupingUpdate(String table, String key, Map<String, ByteIterator> values) {

    return Status.OK;
  }

  private Status insertUpdate(String table, String key, HashMap<String, ByteIterator> values) {
    key.replace("\"", "\\\"");
		/*Set<String>nfields = new HashSet<String>();
		Iterator<String>nit = fields.iterator();
		while (nit.hasNext()) {
			nfields.add(nit.next().replace("\"", "\\\""));
		}
		fields = nfields;*/
    if (null != values) {
      HashMap<String, ByteIterator> nv = new HashMap<String, ByteIterator>();
      Iterator<Entry<String, ByteIterator>> iv = values.entrySet().iterator();
      while (iv.hasNext()) {
        Entry<String, ByteIterator> ev = iv.next();
        String fid = ev.getKey().replace("\"", "\\\"");
        String si = ev.getValue().toString().replace("\"", "\\\"");
        nv.put(fid, new StringByteIterator(si));
      }
      values = nv;
    }

    String str =
        use_dataverse + "insert into dataset " + table + "{\"" + AsterixDBConstant.PRIMARY_KEY + "\":\"" + key + "\"";
    Set<Entry<String, ByteIterator>> kvs = null;
    if (null != values && !values.isEmpty()) {
      kvs = values.entrySet();
      Iterator<Entry<String, ByteIterator>> its = kvs.iterator();
      while (its.hasNext()) {
        Entry<String, ByteIterator> it = its.next();
        str += ",\"" + it.getKey() + "\":\"" + it.getValue().toString() + "\"";
      }
    }
    str += "};";
    return insertDelete(/*key, */str);
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
   * record key.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override public Status insert(String table, String key, Map<String, ByteIterator> values) {
    key.replace("\"", "\\\"");
		/*Set<String>nfields = new HashSet<String>();
		Iterator<String>nit = fields.iterator();
		while (nit.hasNext()) {
			nfields.add(nit.next().replace("\"", "\\\""));
		}
		fields = nfields;*/
    if (null != values) {
      HashMap<String, ByteIterator> nv = new HashMap<String, ByteIterator>();
      Iterator<Entry<String, ByteIterator>> iv = values.entrySet().iterator();
      while (iv.hasNext()) {
        Entry<String, ByteIterator> ev = iv.next();
        String fid = ev.getKey().replace("\"", "\\\"");
        String si = ev.getValue().toString().replace("\"", "\\\"");
        nv.put(fid, new StringByteIterator(si));
      }
      values = nv;
    }

    if (_batching) {
      InsertCache ic = insertCache.get(table);
      if (null == ic) {
        ic = new InsertCache(table);
        insertCache.put(table, ic);
      }
      return ic.addOperator(key, (HashMap<String, ByteIterator>) values);
    } else {
      String str =
          use_dataverse + "insert into dataset " + table + "{\"" + AsterixDBConstant.PRIMARY_KEY + "\":\"" + key + "\"";
      Set<Entry<String, ByteIterator>> kvs = null;
      if (null != values && !values.isEmpty()) {
        kvs = values.entrySet();
        Iterator<Entry<String, ByteIterator>> its = kvs.iterator();
        while (its.hasNext()) {
          Entry<String, ByteIterator> it = its.next();
          str += ",\"" + it.getKey() + "\":\"" + it.getValue().toString() + "\"";
        }
      }
      str += "};";
      return insertDelete(/*key, */str);
    }
  }

  private Status deleteUpdate(String table, String key) {
    key.replace("\"", "\\\"");
    //if this is a "new" table, init HTable object.  Else, use existing one
    String str =
        use_dataverse + " delete $l from dataset " + table + " where $l." + AsterixDBConstant.PRIMARY_KEY + " = \""
            + key + "\"";
    return insertDelete(/*key, */str);
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key   The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  public Status delete(String table, String key) {
    key.replace("\"", "\\\"");
    //		/*Set<String>nfields = new HashSet<String>();
    //		Iterator<String>nit = fields.iterator();
    //		while (nit.hasNext()) {
    //			nfields.add(nit.next().replace("\"", "\\\""));
    //		}
    //		fields = nfields;*/
    //		HashMap<String, ByteIterator> nv = new HashMap<String, ByteIterator>();
    //		Iterator<Entry<String, ByteIterator>>iv = values.entrySet().iterator();
    //		while (iv.hasNext()) {
    //			Entry<String, ByteIterator>ev = iv.next();
    //			String fid = ev.getKey().replace("\"", "\\\"");
    //			String si = ev.getValue().toString().replace("\"", "\\\"");
    //			nv.put(fid, new StringByteIterator(si));
    //		}
    //		values = nv;
    if (_batching) {
      DeleteCache dc = deleteCache.get(table);
      if (null == dc) {
        dc = new DeleteCache(table);
        deleteCache.put(table, dc);
      }
      return dc.addOperator(key, null);
    } else {
      //if this is a "new" table, init HTable object.  Else, use existing one
      String str =
          use_dataverse + " delete $l from dataset " + table + " where $l." + AsterixDBConstant.PRIMARY_KEY + " = \""
              + key + "\"";
      return insertDelete(/*key, */str);
    }
  }

  public static void main(String[] args) throws Exception {
    AsterixDBClient ac = new AsterixDBClient();
    Properties props = new Properties();
    props.setProperty("dataverse", /*BASIC_URL + */"tpch");
    props.setProperty("debug", "false");
    props.setProperty("batch", "true");
    props.setProperty("round", args[0]);
    props.setProperty("length", args[1]);
    long round = (args[0] == null) ? DEFAULT_LOCAL_COUNT : Integer.parseInt(args[0]);
    int length = (args[1] == null) ? 8 : Integer.parseInt(args[1]);
    if (args.length > 2) {
      props.setProperty("batch", args[2]);
      System.out.println(args[2]);
    }
    ac.setProperties(props);
    ac.init();
    ac.create_schema();
    long start = System.currentTimeMillis();
    //		ac.insert(ac.DEFAULT_TABLE, "helloworld10", null);
    //		ac.insert(ac.DEFAULT_TABLE, "helloworld2", null);
    String bs = String.copyValueOf(head.toCharArray(), 0, length);
    for (long i = 1; i < round; i++) {
      //			System.out.print("\t" + i /*+ "-" + Math.abs(("helloworld" + String.valueOf(i)).hashCode()) % MODIFY_CONNECT_COUNT*/);
      if (i % 10 == 0 || i % 10 == 1) {
        //				System.out.println("*");
        //				continue;
      }
      ac.insert(ac.DEFAULT_TABLE, /*"helloworld"*/bs + String.valueOf(i), null);
    }
    ac.cleanUp();
    long end = System.currentTimeMillis();
    System.out.println("Insert elipsed " + round + "-" + length + ": " + (end - start) /*/ 1000*/);
    start = System.currentTimeMillis();
    boolean olds = ac.getBatch();
    ac.setBatch(false);
    Set<String> fields = new HashSet<String>();
    fields.add("FIELD1");
    fields.add("FIELD5");
    for (long i = 1; i < round / 10; i++) {
      HashMap<String, ByteIterator> values1 = new HashMap<String, ByteIterator>();
      for (int j = 1; j < 11; j++) {
        String fld = "FIELD" + String.valueOf(j);
        String vas = new String(bs + String.valueOf(i) + String.valueOf(j));
        byte[] bv = vas.getBytes();
        ByteIterator val = new StringByteIterator(vas);//new ByteArrayByteIterator(vas.getBytes());
        values1.put(fld, val);
      }
      ac.update("usertable", bs + String.valueOf(i), values1);
      //			HashMap<String, ByteIterator> values2 = new HashMap<String, ByteIterator>();
      //
      //			ac.read("usertable", "helloworld" + String.valueOf(i), fields, values2);
    }
    ac.setBatch(olds);
    end = System.currentTimeMillis();
    System.out.println("Update elipsed " + round + "-" + length + ": " + (end - start) /*/ 1000*/);

    start = System.currentTimeMillis();
    for (long i = 1; i < round / 10; i++) {
      HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
      ac.read("usertable", bs + String.valueOf(i), fields, result);
      Iterator<Entry<String, ByteIterator>> itr = result.entrySet().iterator();
      String outStr = new String();
      while (itr.hasNext()) {
        Entry<String, ByteIterator> it = itr.next();
        if (it.getValue() != null)
          outStr += it.getKey() + ":" + it.getValue().toString() + ";";
      }
      //		System.out.print(outStr);
    }
    ac.cleanUp();
    end = System.currentTimeMillis();
    System.out.println("Read elipsed " + round + "-" + length + ": " + (end - start) /*/ 1000*/);

    start = System.currentTimeMillis();
    for (long j = 1; j < round / 10; j++) {
      Vector<HashMap<String, ByteIterator>> results = new Vector<HashMap<String, ByteIterator>>();
      ac.scan("usertable", bs + String.valueOf(j), length, fields, results);
      for (int i = 0; i < results.size(); i++) {
        HashMap<String, ByteIterator> result = results.get(i);
        Iterator<Entry<String, ByteIterator>> itr = result.entrySet().iterator();
        String outStr = new String();
        while (itr.hasNext()) {
          Entry<String, ByteIterator> it = itr.next();
          outStr += it.getKey() + ":" + it.getValue().toString() + ";";
        }
        //			System.out.print(outStr);
      }
    }
    ac.cleanUp();
    end = System.currentTimeMillis();
    System.out.println("Scan elipsed " + round + "-" + length + ": " + (end - start) /*/ 1000*/);
    start = System.currentTimeMillis();
    for (long i = round - 1; i >= 1; i--) {
      ac.delete("usertable", bs + String.valueOf(i));
    }
    ac.cleanUp();
    end = System.currentTimeMillis();
    System.out.println("Delete elipsed " + round + "-" + length + ": " + (end - start) /*/ 1000*/);

    //		ac.delete(ac.DEFAULT_TABLE, "hello world");
  }

  public static void main1(String[] args) {
    if (args.length != 3) {
      System.out.println("Please specify a threadcount, columnfamily and operation count");
      System.exit(0);
    }

    final int keyspace = 10000; //120000000;

    final int threadcount = Integer.parseInt(args[0]);

    final String dataverse = args[1];

    final int opcount = Integer.parseInt(args[2]) / threadcount;

    Vector<Thread> allthreads = new Vector<Thread>();

    for (int i = 0; i < threadcount; i++) {
      Thread t = new Thread() {
        public void run() {
          try {
            Random random = new Random();

            AsterixDBClient cli = new AsterixDBClient();

            Properties props = new Properties();
            props.setProperty("dataverse", BASIC_URL + dataverse);
            props.setProperty("debug", "true");
            cli.setProperties(props);

            cli.init();
            cli.create_schema();

            //HashMap<String,String> result=new HashMap<String,String>();

            long accum = 0;

            for (int i = 0; i < opcount; i++) {
              int keynum = random.nextInt(keyspace);
              String key = "user" + keynum;
              long st = System.currentTimeMillis();
              Status rescode;
							/*
                            HashMap hm = new HashMap();
                            hm.put("field1","value1");
                            hm.put("field2","value2");
                            hm.put("field3","value3");
                            rescode=cli.insert("table1",key,hm);
                            HashSet<String> s = new HashSet();
                            s.add("field1");
                            s.add("field2");

                            rescode=cli.read("table1", key, s, result);
                            //rescode=cli.delete("table1",key);
                            rescode=cli.read("table1", key, s, result);
							 */
              HashSet<String> scanFields = new HashSet<String>();
              scanFields.add("field1");
              scanFields.add("field3");
              Vector<HashMap<String, ByteIterator>> scanResults = new Vector<HashMap<String, ByteIterator>>();
              rescode = cli.scan("table1", "user2", 20, null, scanResults);

              long en = System.currentTimeMillis();

              accum += (en - st);

              if (rescode != Status.OK) {
                System.out.println("Error " + rescode + " for " + key);
              }

              if (i % 1 == 0) {
                System.out.println(i + " operations, average latency: " + (((double) accum) / ((double) i)));
              }
            }

            //System.out.println("Average latency: "+(((double)accum)/((double)opcount)));
            //System.out.println("Average get latency: "+(((double)cli.TotalGetTime)/((double)cli.TotalGetOps)));
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      };
      allthreads.add(t);
    }

    long st = System.currentTimeMillis();
    for (Thread t : allthreads) {
      t.start();
    }

    for (Thread t : allthreads) {
      try {
        t.join();
      } catch (InterruptedException e) {
      }
    }
    long en = System.currentTimeMillis();

    System.out.println(
        "Throughput: " + ((1000.0) * (((double) (opcount * threadcount)) / ((double) (en - st)))) + " ops/sec");
  }
}

/* For customized vim control
 * set autoindent
 * set si
 * set shiftwidth=4
 */

