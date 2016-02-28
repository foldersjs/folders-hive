var thrift = require('thrift');
var hive = require('../lib/gen-nodejs/TCLIService');
var ttypes = require('../lib/gen-nodejs/TCLIService_types');

var HiveThriftClient = function(options, callback) {
  this.connect(options, callback);
};

module.exports = HiveThriftClient;

/*
 * Connect to Hive database - callback : callback(error, session) function - error : error - session : opened session
 */
HiveThriftClient.prototype.connect = function connect(options, callback) {
  var self = this;

  self.connection = thrift.createConnection(options.host, options.port);
  self.client = thrift.createClient(hive, self.connection, {
    auth : options.auth,
    timeout : options.timeout
  });

  /* Handle connection errors */
  self.connection.on('error', function(error) {
    console.error('connect error : ' + error);
    if (callback)
      callback(error, null);
  });

  self.connection.on('connect', function() {
    openSessionThrift(self.client, options, function(error, response, protocol) {
      if (error) {
        console.error("OpenSession error = " + JSON.stringify(error));
        self.session = null;
      } else {
        console.info("Session opened for user " + options.username + " with protocol value = " + protocol);

        // TODO one thing to notice, is there any expire time for the session ?
        // if so , we need to re-open a new session when execute thrift request.
        self.session = response.sessionHandle;
      }
      if (callback)
        callback(error, response.sessionHandle);
    });
  });

};

/*
 * Disconnect to hive database - session : The opened session - callback : callback(status) function - status : status
 */
HiveThriftClient.prototype.disconnect = function disconnect(callback) {

  var session = this.session;
  var connection = this.connection;
  var client = this.client;

  /* Closing hive session */
  closeSessionThrift(client, session, function(status) {
    if (status) {
      console.error("disconnect error = " + JSON.stringify(status));
    } else {
      console.info('session closed');
    }

    /* Handle disconnect success */
    connection.on('end', function(error) {
      logger.info('disconnect success');
    });

    /* Closing thrift connection */
    connection.end();
    if (callback)
      callback(status);
  });

  this.client = null;
  this.connection = null;
  this.session = null;

};

// callback return the databases name array.
HiveThriftClient.prototype.getSchemasNames = function getSchemasNames(cb) {
  var session = this.session;
  var client = this.client;

  getSchemasThrift(client, session, function(error, response) {
    if (error) {
      console.error('show shemas error', error);
      return cb(error, null);
    }

    getRowColumnsByColumnName(client, response.operationHandle, 'TABLE_SCHEM', function(error, response) {
      cb(error, response);
    });

  });
};

HiveThriftClient.prototype.getTablesNames = function getTablesNames(schemaName, callback) {
  var session = this.session;
  var client = this.client;

  getTablesThrift(client, session, schemaName, function(error, response) {
    if (error) {
      console.error("getTablesNames error = " + JSON.stringify(error));
      callback(error, response);
    } else {
      getRowColumnsByColumnName(client, response.operationHandle, 'TABLE_NAME', function(error, response) {
        callback(error, response);
      });
    }
  });
}

HiveThriftClient.prototype.getTableColumns = function getTableColumns(schemaName, tableName, callback) {
  var session = this.session;
  var client = this.client;

  getColumnsThrift(client, session, schemaName, tableName, function(error, response) {
    if (error) {
      callback(error, response);
    } else {
      var tableColumnsToSelect = [ 'TABLE_SCHEM', 'TABLE_NAME', 'COLUMN_NAME', 'TYPE_NAME', 'IS_NULLABLE' ];
      getRowsByColumnNames(client, response.operationHandle, tableColumnsToSelect, function(error, response) {
        callback(error, response);
      });
    }
  });
}

HiveThriftClient.prototype.getTableRecords = function getTableRecords(schemaName, tableName, callback) {
  // FIXME: Escape db/tbname as needed.
  var sql = 'SELECT * FROM ' + schemaName + '.' + tableName;

  this.executeSelect(sql, callback);
}

HiveThriftClient.prototype.showCreateTable = function showCreateTable(schemaName, tableName, callback) {
  var session = this.session;
  var client = this.client;

  // FIXME: Escape db/tbname as needed.
  var sql = 'SHOW CREATE TABLE ' + schemaName + '.' + tableName;

  this.rawExecuteStatement(sql, function(error, response) {
    if (error) {
      console.error("executeSelect error = " + JSON.stringify(error));
      callback(error, response);
    } else {
      getRowColumnsByColumnName(client, response.operationHandle, 'createtab_stmt', function(error, response) {
        if (error)
          return callback(error, null);

        callback(error, response.join('\n'));
      });
    }
  });
}

/*
 * Execute a select statement - session : The operation to fetch rows from - statement : The statement to be executed -
 * maxRows : max row number to be fetched - callback : callback(error, result) function - error : error - result :
 * result of the executeSelect
 */
HiveThriftClient.prototype.executeSelect = function executeSelect(selectStatement, callback) {
  var client = this.client;
  this.rawExecuteStatement(selectStatement, function(error, response) {
    if (error) {
      console.error("executeSelect error = " + JSON.stringify(error));
      callback(error, response);
    } else {
      getRowsByColumnNames(client, response.operationHandle, null, function(error, response) {
        callback(error, response);
      });
    }
  });
}

/*
 * Execute a statement, using the raw Thrift Hive API - session : The opened session - statement : The statement to be
 * executed - callback : callback(error, result) function - error : error - result : result of the rawExecuteStatement
 */
HiveThriftClient.prototype.rawExecuteStatement = function rawExecuteStatement(statement, callback) {
  var session = this.session;
  var client = this.client;

  executeStatementThrift(client, session, statement, function(error, response) {
    if (error) {
      console.error("executeStatement error = " + JSON.stringify(error));
      callback(error, null);
    } else if (response.status.statusCode == 3) {
      console.error("executeStatement error = " + JSON.stringify(response.status));
      callback(response.status, null);
    } else {
      callback(null, response);
    }

  });
}

// Open Hive session
function openSessionThrift(client, config, callback) {
  var protocol = ttypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V7
  var openSessReq = new ttypes.TOpenSessionReq();
  openSessReq.username = config.username;
  openSessReq.password = config.password;
  openSessReq.client_protocol = protocol;
  client.OpenSession(openSessReq, function(error, response) {
    callback(error, response, protocol);
  });
}

/* Close Hive session */
function closeSessionThrift(client, session, callback) {
  var closeSessReq = new ttypes.TCloseSessionReq();
  closeSessReq.sessionHandle = session;
  client.CloseSession(closeSessReq, function(error, response) {
    callback(error, response);
  });
}

function getSchemasThrift(client, session, callback) {
  var request = new ttypes.TGetSchemasReq();
  request.sessionHandle = session;
  client.GetSchemas(request, callback);
}

/* Execute getTables action */
function getTablesThrift(client, session, schemaName, callback) {
  var request = new ttypes.TGetTablesReq();
  request.sessionHandle = session;
  request.schemaName = schemaName;
  client.GetTables(request, function(error, response) {
    callback(error, response)
  });
}

/* Execute get table Columns action */
function getColumnsThrift(client, session, schemaName, tableName, callback) {
  var request = new ttypes.TGetColumnsReq();
  request.sessionHandle = session;
  request.schemaName = schemaName;
  request.tableName = tableName;
  client.GetColumns(request, function(error, response) {
    callback(error, response);
  });
}

/* Execute HiveQL Statement */
function executeStatementThrift(client, session, statement, callback) {
  var request = new ttypes.TExecuteStatementReq();
  request.sessionHandle = session;
  request.statement = statement;
  request.runAsync = false;
  client.ExecuteStatement(request, function(error, response) {
    callback(error, response);
  });
}

// Execute GetResultSetMetadata action
function getResultSetMetadataThrift(client, operation, callback) {
  var request = new ttypes.TGetResultSetMetadataReq();
  request.operationHandle = operation;
  client.GetResultSetMetadata(request, function(error, response) {
    callback(error, response);
  });
}

// Execute fetchRow action on operation result
// TODO how to set the maxRows,how to read all the Rows
function fetchRowsThrift(client, operation, maxRows, callback) {
  var request = new ttypes.TFetchResultsReq();
  request.operationHandle = operation;
  request.orientation = ttypes.TFetchOrientation.FETCH_NEXT;
  request.maxRows = maxRows;
  client.FetchResults(request, function(error, response) {
    callback(error, response)
  });
}

// return array of the specified column
/*
 * ['row1-columnX-value', 'row2-columnX-value', .... rowN-columnX-value ]
 */
// TODO check if we could get specific colummns rather than all the columns here.
// Maybe we should always select the specific colummns in SQL, rather than filter from Fetch Result
function getRowColumnsByColumnName(client, operation, columnName, callback) {
  getResultSetMetadataThrift(client, operation, function(error, responseMeta) {
    if (error) {
      callback(error, null);
    } else {
      fetchRowsThrift(client, operation, 1000, function(error, responseFetch) {
        if (error) {
          callback(error, null);
        } else {
          var result;
          var metaColumns = responseMeta.schema.columns;
          var rowColumns = responseFetch.results.columns;
          var currentMeta, currentRow;
          var type = '';
          for (var i = 0; i < metaColumns.length; i++) {

            currentMeta = metaColumns[i];
            currentRow = rowColumns[i];
            type = getReverseTColumn(currentMeta.typeDesc.types[0].primitiveEntry.type);

            if (currentMeta.columnName === columnName) {
              result = currentRow[type].values;
              break;
            }
          }
          callback(error, result);
        }
      });
    }
  });
}

// return a array of the operation, just like the tables display on CLI.
// the first element of the array, is a array of the column names.
// the left elements of the array, every element is array of the one actual row data.
/*
 * [ ['columnName1', 'ColumnName2'], ['row1-column1-value', 'row1-column2-value'], ['row2-column1-value',
 * 'row2-column2-value'], ... , ['rowN-column1-value', 'rowN-column2-value'] ]
 */
// use the columnNamesToSelect to filter column, specify the columns we want to select
// if not specify, will return all the columns.
function getRowsByColumnNames(client, operation, columnNamesToSelect, callback) {

  getResultSetMetadataThrift(client, operation, function(error, responseMeta) {
    if (error) {
      return callback(error, null);
    }

    fetchRowsThrift(client, operation, 50, function(error, responseFetch) {
      if (error) {
        return callback(error, null);
      }

      var metaColumns = responseMeta.schema.columns;
      var rowColumns = responseFetch.results.columns;

      var columnNames = [];
      var columnPos = []; // actual pos in rowColumns
      var columnTypes = [];
      var currentMeta;
      // filter the column, store the column name,type,post
      for (var i = 0; i < metaColumns.length; i++) {
        currentMeta = metaColumns[i];

        if (columnNamesToSelect && columnNamesToSelect.length > 0
            && columnNamesToSelect.indexOf(currentMeta.columnName) < 0) {
          // filter the column.
          continue;
        }

        columnNames.push(currentMeta.columnName);
        columnTypes.push(getReverseTColumn(currentMeta.typeDesc.types[0].primitiveEntry.type))
        columnPos.push(i);
      }

      if (columnNames.length == 0) {
        return callback('no matched columns', null);
      }

      var columnSize = columnNames.length;
      var rowSize = rowColumns[columnPos[0]][columnTypes[0]].values.length;
      var result = [];
      var row = [];
      // push title
      result.push(columnNames);
      // push records.
      for (var i = 0; i < rowSize; i++) {
        row = [];
        for (var j = 0; j < columnSize; j++) {
          row.push(rowColumns[columnPos[j]][columnTypes[j]].values[i]);
        }
        // push row
        result.push(row);
      }

      callback(error, result);
    });

  });

}

/* Get TColumnValue from TTypeID, used to retrieve data from fetch (TColumnValue) with metadata knowledge (TTypeID) */
function getReverseTColumn(numericValue) {
  switch (numericValue) {
  case ttypes.TTypeId.BOOLEAN_TYPE:
    return 'boolVal';
  case ttypes.TTypeId.TINYINT_TYPE:
    return 'byteVal';
  case ttypes.TTypeId.SMALLINT_TYPE:
    return 'i16Val';
  case ttypes.TTypeId.INT_TYPE:
    return 'i32Val';
  case ttypes.TTypeId.BIGINT_TYPE:
    return 'i64Val';
  case ttypes.TTypeId.FLOAT_TYPE:
    return 'doubleVal';
  case ttypes.TTypeId.DOUBLE_TYPE:
    return 'doubleVal';
  case ttypes.TTypeId.STRING_TYPE:
    return 'stringVal';
  case ttypes.TTypeId.TIMESTAMP_TYPE:
    return 'i64Val';
  case ttypes.TTypeId.BINARY_TYPE:
    return 'stringVal';
  case ttypes.TTypeId.ARRAY_TYPE:
    return 'stringVal';
  case ttypes.TTypeId.MAP_TYPE:
    return 'stringVal';
  case ttypes.TTypeId.STRUCT_TYPE:
    return 'stringVal';
  case ttypes.TTypeId.UNION_TYPE:
    return 'stringVal';
  case ttypes.TTypeId.USER_DEFINED_TYPE:
    return 'stringVal';
  case ttypes.TTypeId.DECIMAL_TYPE:
    return 'stringVal';
  case ttypes.TTypeId.NULL_TYPE:
    return 'stringVal';
  case ttypes.TTypeId.DATE_TYPE:
    return 'stringVal';
  case ttypes.TTypeId.VARCHAR_TYPE:
    return 'stringVal';
  case ttypes.TTypeId.CHAR_TYPE:
    return 'stringVal';
  case ttypes.TTypeId.INTERVAL_YEAR_MONTH_TYPE:
    return 'stringVal';
  case ttypes.TTypeId.INTERVAL_DAY_TIME_TYPE:
    return 'stringVal';
  default:
    return null;
  }
}