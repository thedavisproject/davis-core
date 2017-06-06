const { expect } = require('chai');
const csvParse = require('../../../../src/data/import/parse/csvParser');
const StreamTest = require('streamtest');

describe('CSV Parser', function(){

  const importFilePath = __dirname + '/files/data.csv';

  it('should parse the file', function(done){

    const outStream = StreamTest['v2'].toObjects(function(error, results){
      expect(error).to.be.null;
      expect(results).to.have.length(2);
      expect(results[0]).to.deep.equal({
        Location: 'MA',
        Year: '2012',
        Percent: '.5'
      });
      expect(results[1]).to.deep.equal({
        Location: 'NY',
        Year: '2013',
        Percent: '.7'
      });
      done();
    });

    csvParse(importFilePath)
      .pipe(outStream);
  });

});
