/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.functions.response;

import org.jetbrains.annotations.NotNull;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.sql.data.model.ExprFloatValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionEngine;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Default implementation of SqlFunctionResponseHandle.
 */
public class DefaultSqlFunctionResponseHandle implements SqlFunctionResponseHandle {

  private final JSONObject responseObject;
  private Iterator<ExprValue> responseIterator;
  private ExecutionEngine.Schema schema;

  /**
   * Constructor.
   *
   * @param responseObject Spark responseObject.
   */
  public DefaultSqlFunctionResponseHandle(JSONObject responseObject) {
    this.responseObject = responseObject;
    constructIteratorAndSchema();
  }

  private void constructIteratorAndSchema() {
    List<ExprValue> result = new ArrayList<>();
    List<ExecutionEngine.Schema.Column> columnList;
    if (responseObject.has("data")) {
      JSONObject items = responseObject.getJSONObject("data");
      columnList = getColumnList(items.getJSONArray("schema"));
      for(int i=0; i<items.getJSONArray("result").length(); i++){
        JSONObject row = new JSONObject(items.getJSONArray("result").get(i).toString().replace("'", "\""));
        LinkedHashMap<String, ExprValue> linkedHashMap = extractRow(row, columnList);
        result.add(new ExprTupleValue(linkedHashMap));
      }
    } else {
      throw new RuntimeException("Unexpected result during spark sql query execution");
    }
    this.schema = new ExecutionEngine.Schema(columnList);
    this.responseIterator = result.iterator();
  }

  @NotNull
  private static LinkedHashMap<String, ExprValue> extractRow(JSONObject row, List<ExecutionEngine.Schema.Column> columnList) {
    LinkedHashMap<String, ExprValue> linkedHashMap = new LinkedHashMap<>();
    for (ExecutionEngine.Schema.Column column : columnList) {
      ExprType type = column.getExprType();
      if(type == ExprCoreType.STRING)
        linkedHashMap.put(column.getName(), new ExprStringValue(row.getString(column.getName())));
      else if(type == ExprCoreType.INTEGER) {
        linkedHashMap.put(column.getName(), new ExprIntegerValue(row.getInt(column.getName())));
      } else if(type == ExprCoreType.FLOAT) {
        linkedHashMap.put(column.getName(), new ExprFloatValue(row.getFloat(column.getName())));
      } else if(type == ExprCoreType.LONG) {
        linkedHashMap.put(column.getName(), new ExprLongValue(row.getLong(column.getName())));
      } else {
        throw new RuntimeException("Invalid data type");
      }
    }
    return linkedHashMap;
  }


  private List<ExecutionEngine.Schema.Column> getColumnList(JSONArray schema) {
    List<ExecutionEngine.Schema.Column> columnList = new ArrayList<>();
    for(int i=0; i<schema.length(); i++) {
      JSONObject column = new JSONObject(schema.get(i).toString().replace("'", "\""));
      columnList.add(new ExecutionEngine.Schema.Column(column.get("column_name").toString(), column.get("column_name").toString(), getDataType(column.get("data_type").toString())));
    }
    return columnList;
  }

  private ExprCoreType getDataType(String sparkDataType) {
    switch (sparkDataType) {
      case "integer":
        return ExprCoreType.INTEGER;
      case "byte":
        return ExprCoreType.BYTE;
      case "short":
        return ExprCoreType.SHORT;
      case "long":
        return ExprCoreType.LONG;
      case "float":
        return ExprCoreType.FLOAT;
      case "double":
        return ExprCoreType.DOUBLE;
      case "boolean":
        return ExprCoreType.BOOLEAN;
      case "date":
        return ExprCoreType.DATE;
      case "timestamp":
        return ExprCoreType.TIMESTAMP;
      case "string":
      case "varchar":
      case "char":
      default:
        return ExprCoreType.STRING;
    }
  }



  @Override
  public boolean hasNext() {
    return responseIterator.hasNext();
  }

  @Override
  public ExprValue next() {
    return responseIterator.next();
  }

  @Override
  public ExecutionEngine.Schema schema() {
    return schema;
  }
}
