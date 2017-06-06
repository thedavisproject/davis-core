const fs = require('fs'),
  parse = require('csv-parse');

module.exports = function(filePath){

  return fs.createReadStream(filePath)
    .pipe(parse({ columns: true }));
};
