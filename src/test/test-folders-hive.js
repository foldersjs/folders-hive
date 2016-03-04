var FoldersHive = new require('../folders-hive');

var prefix = 'folders.io_0:hive';

var config = {
  "host" : "130.211.140.182",
  "port" : 10000,
  "auth" : "none", // none, nosasl
  "username": "hive",
  "password" : "hive-password"
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

    foldersHive.ls('/folders', function cb(error, tables) {
      if (error) {
        console.log("error in ls database folders");
        console.log(error);
      }
      console.log("ls tables success, ", tables);

      foldersHive.ls('/folders/test', function cb(error, metadata) {
        if (error) {
          console.log('error in ls table metadata');
          console.log(error);
        }

        console.log('ls metadata success, ', metadata);

        foldersHive.cat('/folders/test/columns.md', function cb(error, columns) {
          if (error) {
            console.log('error in cat table columns');
            console.log(error);
          }

          console.log('cat table columns success,', columns.size);

          foldersHive.cat('/folders/test/create_table.md', function cb(error, columns) {
            if (error) {
              console.log('error in cat create table SQL');
              console.log(error);
            }

            console.log('cat create table SQL success, size:', columns.size);

            foldersHive.cat('/folders/test/select.md', function cb(error, records) {
              if (error) {
                console.log('error in cat table record');
                console.log(error);
              }

              console.log('cat table record success, size:', records.size);

              foldersHive.disconnect();
            });
          });
        });

      });

    });

  });
});
