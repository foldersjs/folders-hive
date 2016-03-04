Folders
=============

This node.js package implements the folders.io synthetic file system.

This Folders Module is directly connecting to The HiveServer2 using thrift.

The Thrift interface definition language (IDL) for HiveServer2 is available at https://github.com/apache/hive/blob/trunk/service/if/TCLIService.thrift.

Thrift documentation is available at http://thrift.apache.org/docs/.

Module can be installed via "npm install folders-hive".

##Folders-hive

Basic Usage

### Configuration

In order to connect to HiveServer2, specify the following args.

```json
{
  "host" : "hive_server2_hostname",
  "port" : "hive_server2_port", 
  "username" : "conn_username",
  "password" : "conn_password",
  "auth" : "nosasl"
}
```

**Auth mode**

we now support only 'None'(uses plain SASL), NOSASL Authentication.

[HiveServer2 Authentication/Security Configuration](https://cwiki.apache.org/confluence/display/Hive/Setting+Up+HiveServer2#SettingUpHiveServer2-Authentication/SecurityConfiguration)


### Constructor

Provider constuctor, could pass the special option/param in the config param.

```js
var prefix = 'folders.io_0:hive';

var config = {
  "host" : "hive_server2_hostname",
  "port" : "hive_server2_port", 
  "username" : "conn_username",
  "password" : "conn_password",
  "auth" : "nosasl"
};

var foldersHive = new FoldersHive(prefix, config);
```

### ls

ls the database, tables as folders.

```js
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
FoldersHive.prototype.ls = function(path, cb)

foldersHive.ls('/', function cb(error, databases) {
  if (error) {
    console.log("error in ls /");
    console.log(error);
  }
  console.log("ls databases success, ", databases);
};

```


### Cat

cat columns metadata,records or create sql of the specified table.

```js
/**
 * @param uri, the file uri to cat 
 * @param cb, callback  function(err, result) function.
 *    example for result.
 *    {
 *      stream: .., // a readable 'request' stream
 *      size : .. , // file size
 *      name: path
 *    }
 *
 * cat(uri,cb) 
 */
 
foldersHive.cat('/folders/test/columns.md', function cb(error,columns) {
  if (error) {
    console.log('error in cat table columns');
    console.log(error);
  }
  console.log('cat table columns success, \n', columns);
});

```


currently we support three types of files. ['columns.md', 'create_table.md', 'select.md']

- 'columns.md' 

The file data will be a markdown table, show the metadata of columns, include the column type, name, nullable...

```txt
| TABLE_SCHEM | TABLE_NAME | COLUMN_NAME | TYPE_NAME | IS_NULLABLE |
| ----------- | ---------- | ----------- | --------- | ----------- |
| folders     | test       | col1        | STRING    | YES         |
| folders     | test       | col2        | STRING    | YES         |
```

- 'create_table.md' 

The file data will be a sql, show the create sql statement of the table.

```sql
CREATE EXTERNAL TABLE `folders.test`(
  `col1` string, 
  `col2` string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://cluster-1-m:8020/tmp/example'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='false', 
  'numFiles'='0', 
  'numRows'='-1', 
  'rawDataSize'='-1', 
  'totalSize'='0', 
  'transient_lastDdlTime'='1452669800')
```

- select.md

The file show a **limited number(10)** rows of the table records.

```txt
| test.col1 | test.col2 |
| --------- | --------- |
| row1-col1 | row1-col2 |
| row2-col1 | row2-col2 |
```