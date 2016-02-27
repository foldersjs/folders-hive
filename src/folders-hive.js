var HiveThriftClient = require('./hiveThriftClient');

var assert = require('assert');
var Readable = require('stream').Readable;
var tableFormatter = require('markdown-table');

var DEFAULT_HIVE_PREFIX = "/folders.io_0:hive/";

var FoldersHive = function(prefix, options, callback) {
  assert.equal(typeof (options), 'object', "argument 'options' must be a object");

  if (prefix && prefix.length && prefix.substr(-1) != '/')
    prefix += '/';
  this.prefix = prefix || DEFAULT_HIVE_PREFIX;

  this.configure(options, callback);
};

module.exports = FoldersHive;

FoldersHive.prototype.configure = function(options, callback) {
  this.host = options.host;
  this.port = options.port;
  this.username = options.username || 'anonymous';
  this.password = options.password || '';
  this.auth = options.auth || 'nosasl';
  this.timeout = options.timeout = "10000";

  this.client = new HiveThriftClient(options, callback);
};

FoldersHive.prototype.disconnect = function disconnect(callback) {
  this.client.disconnect(callback);
}

FoldersHive.prototype.features = FoldersHive.features = {
  cat : true,
  ls : true,
  write : false,
  server : false
};

FoldersHive.isConfigValid = function(config, cb) {
  assert.equal(typeof (config), 'object', "argument 'config' must be a object");

  assert.equal(typeof (cb), 'function', "argument 'cb' must be a function");

  var checkConfig = config.checkConfig;
  if (checkConfig == false) {
    return cb(null, config);
  }

  // TODO check access credentials and test conn if needed.

  return cb(null, config);
};

// remove the prefix from path if exist
FoldersHive.prototype.getHivePath = function(path, prefix) {
  path = (path == '/' ? null : path.slice(1));

  if (path == null) {
    return null;
  }

  var parts = path.split('/');
  var prefixPath = parts[0];
  if (prefix && prefix[0] == '/')
    prefixPath = '/' + prefixPath;
  prefixPath = prefixPath + '/';

  // if the path start with the prefix, remove the prefix string.
  if (prefixPath == prefix) {
    parts = parts.slice(1, parts.length);
  }

  var out = {};
  if (parts.length > 0)
    out.database = parts[0];
  if (parts.length > 1)
    out.table = parts[1];
  if (parts.length > 2)
    out.tableMetadata = parts[2];

  return out;
};

/**
 * list db metadata in folders.io format
 * 
 * @param uri,
 *          the uri for db. /${database}/${table}/ eg:
 *          <li>'/' , show the root path, show the databases/schemas</li>
 *          <li>'/test-db', show all the tables in database 'test-db' </li>
 *          <li>'/test-db/test-table', show the metadata of table we support 'test-table' in database 'test-db';</li>
 *          all the path could also start with {prefix}, '/folders.io_0:hive/test-db'
 * @param cb,
 *          callback function(err, result) function. result will be a file info array. [{}, ... {}] <br>
 *          a example file information <code>
 *            { 
 *               name: 'default',
 *               fullPath: 'default',
 *               meta: {},
 *               uri: 'folders.io_0:hive/default',
 *               size: 0,
 *               extension: '+folder',
 *               modificationTime: 0
 *            }
 *            </code>
 */
FoldersHive.prototype.ls = function(path, cb) {
  path = this.getHivePath(path, this.prefix);
  if (path == null || !path.database) {
    showDatabases(this.client, this.prefix, cb);
  } else if (!path.table) {
    showTables(this.client, this.prefix, path.database, cb);
  } else {
    showTableMetas(this.prefix, path.database + '/' + path.table, cb);
  }
};

FoldersHive.prototype.cat = function(path, cb) {
  path = this.getHivePath(path, this.prefix);

  if (path == null || !path.database || !path.table || !path.tableMetadata) {
    var error = "please specify the the database,table and metadata you want in path";
    console.log(error);
    return cb(error, null);
  }

  if (path.tableMetadata == 'select.md') {
    showTableSelect(this.client, this.prefix, path.database, path.table, cb);
  } else if (path.tableMetadata == 'create_table.md') {
    showCreateTable(this.client, this.prefix, path.database, path.table, cb);
  } else if (path.tableMetadata == 'columns.md') {
    showTableColumns(this.client, this.prefix, path.database, path.table, cb);
  } else {
    // NOTES, now supported now
    cb("not supported yet", null);
  }
}

var showDatabases = function(client, prefix, cb) {
  client.getSchemasNames(function(error, databases) {
    if (error) {
      console.log('show shemas error', error);
      return cb(error, null);
    }

    if (!databases) {
      return cb('databases null', null);
    }

    cb(null, dbAsFolders(prefix, databases));
  });
};

var dbAsFolders = function(prefix, dbs) {
  var out = [];
  for (var i = 0; i < dbs.length; i++) {
    var db = dbs[i];
    var o = {
      name : db
    };
    o.fullPath = o.name;
    o.meta = {};
    o.uri = prefix + o.fullPath;
    o.size = 0;
    o.extension = '+folder';
    // o.type = "text/plain";
    o.modificationTime = 0;
    out.push(o);
  }
  return out;
}

var showTables = function(client, prefix, dbName, cb) {
  client.getTablesNames(dbName, function(error, tables) {
    if (error) {
      return cb(error, null);
    }

    if (!tables) {
      return cb('null tables,', tables);
    }

    cb(null, tbAsFolders(prefix, dbName, tables));
  });

};

var tbAsFolders = function(prefix, dbName, tbs) {
  var out = [];
  for (var i = 0; i < tbs.length; i++) {
    var table = tbs[i];
    var o = {
      name : table
    };
    o.fullPath = dbName + '/' + o.name;
    o.meta = {};
    o.uri = prefix + o.fullPath;
    o.size = 0;
    o.extension = '+folder';
    // o.type = "text/plain";
    o.modificationTime = 0;
    out.push(o);
  }
  return out;
}

var showTableMetas = function(prefix, path, cb) {

  var metadatas = [ 'columns', 'create_table', 'select' ];

  var out = [];
  for (var i = 0; i < metadatas.length; i++) {
    var o = {
      name : metadatas[i] + '.md'
    };
    o.fullPath = path + '/' + o.name;
    o.meta = {};
    o.uri = prefix + o.fullPath;
    // FIXME can't get the size.
    o.size = 0;
    o.extension = 'md';
    o.type = "text/markdown";
    o.modificationTime = 0;
    out.push(o);
  }

  cb(null, out);
};

var showTableSelect = function(client, prefix, dbName, tbName, cb) {

};

var showCreateTable = function(client, prefix, dbName, tbName, cb) {
  client.showCreateTable(dbName, tbName, function(error, createTableSQL) {
    if (error) {
      return cb(error, null);
    }

    if (!createTableSQL) {
      return cb('null tables,', null);
    }
    var foramttedCreateTableSQL = "```sql" + '\n' + createTableSQL + '\n' + "```";
    console.log('showCreateTable result:');
    console.log(foramttedCreateTableSQL);
    callbackCatResult('create_table.md', foramttedCreateTableSQL, cb);

  });
};

var showTableColumns = function(client, prefix, dbName, tbName, cb) {

  client.getTableColumns(dbName, tbName, function(error, columns) {
    if (error) {
      return cb(error, null);
    }

    if (!columns) {
      return cb('null tables,', null);
    }

    var formattedColumnsData = tableFormatter(columns);// ,{'align': 'c'}
    console.log('showTableColumns result:');
    console.log(formattedColumnsData);
    callbackCatResult('columns.md', formattedColumnsData, cb);

  });
};

var showGenericResult = function(name, data, columns, cb) {
  // convert the title of columns.
  var title = [];
  for (var i = 0; i < columns.length; i++) {
    title.push(columns[i].name);
  }
  // insert the titils line before the first row
  data.unshift(title);

  // format the columns data include the title into markdown table
  var formattedColumnsData = tableFormatter(data);// ,{'align': 'c'}

  callbackCatResult(name, formattedColumnsData, cb);
};

var callbackCatResult = function(name, data, cb) {

  // create a readable stream
  var stream = new Readable();
  stream.push(data);
  stream.push(null);

  cb(null, {
    'stream' : stream,
    'size' : data.length,
    'name' : name
  });

}
