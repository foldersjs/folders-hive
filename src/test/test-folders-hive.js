var FoldersHive = new require('../folders-hive');

var prefix = 'folders.io_0:hive';

var config = {
  "host" : "130.211.140.182",
  "port" : 10000
};

var foldersHive = new FoldersHive(prefix, config, function(err, session) {
  if (err) {
    console.error('setup Folders Hive error,', err);
    return;
  }

  foldersHive.ls('/', function cb(error, databases) {
    if (error) {
      console.log("error in ls /");
      console.log(error);
    }

    console.log("ls databases success, ", databases);

    foldersHive.disconnect();
    // foldersHive.ls('/folders', function cb(error, tables) {
    // if (error) {
    // console.log("error in ls database folders");
    // console.log(error);
    // }
    // console.log("ls tables success, ", tables);
    //
    // foldersHive.ls('/folders/test', function cb(error, metadata) {
    // if (error) {
    // console.log('error in ls table metadata');
    // console.log(error);
    // }
    //
    // console.log('ls metadata success, ', metadata);
    // foldersHive.cat('/folders/test/columns', function cb(error,
    // columns) {
    // if (error) {
    // console.log('error in cat table columns');
    // console.log(error);
    // }
    //
    // console.log('cat table columns success, \n', columns);
    // });
    // })
    //
    // });

  });
});