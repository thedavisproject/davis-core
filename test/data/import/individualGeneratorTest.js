const chai = require('chai');

const {expect, assert} = chai;

const StreamTest = require('streamtest');
const { variable, attribute } = require('davis-model');
const entityRepoStub = require('../../stub/entityRepositoryStub');

const individualGeneratorFac = require('../../../src/data/import/individualGenerator');

describe('Import process', function() {

  const commonTestEntities = [
    variable.newCategorical(72, 'Location'),
    variable.newCategorical(98, 'Year'),
    variable.newNumerical(600, 'Percent'),
    variable.newText(700, 'Name'),
    variable.newCategorical(73, 'Location-Keyed', { key: 'loc'}),
    attribute.new(4, '2012', 98),
    attribute.new(5, '2013', 98),
    attribute.new(45, 'MA', 72),
    attribute.new(76, 'NY', 72),
    attribute.new(77, 'MA', 73, { key: 'MA-Keyed' }),
    attribute.new(78, 'NY', 73, { key: 'NY-Keyed' })
  ];

  it('should successfully map data to individuals when mappings are present', function(done) {

    const schema = [
      {
        variable: 72,
        attributes: [ 45, 76 ]
      },
      {
        variable: 98,
        attributes: [4, 5]
      },
      {
        variable: 600
      },
      {
        variable: 700
      }
    ];

    const entityRepository = entityRepoStub(commonTestEntities);

    const {rawToIndividuals} = individualGeneratorFac({entityRepository});

    const dataStream = StreamTest['v2'].fromObjects([{
      Location: 'MA',
      Year: '2012',
      Percent: .5,
      Name: 'Foo'
    }, {
      Location: 'NY',
      Year: '2013',
      Percent: .7,
      Name: 'Bar'
    }]);

    const outStream = StreamTest['v2'].toObjects(
      function(error, results) {
        expect(error).to.be.null;
        expect(results).to.have.length(2);

        expect(results[0].id).to.equal(1);
        expect(results[0].dataSet).to.equal(56);
        expect(results[0].facts).to.have.length(4);

        expect(results[0].facts[0]).to.deep.equal({
          variable: 72,
          type: variable.types.categorical,
          attribute: 45
        });

        expect(results[0].facts[1]).to.deep.equal({
          variable: 98,
          type: variable.types.categorical,
          attribute: 4
        });

        expect(results[0].facts[2]).to.deep.equal({
          variable: 600,
          type: variable.types.numerical,
          value: 0.5
        });

        expect(results[0].facts[3]).to.deep.equal({
          variable: 700,
          type: variable.types.text,
          value: 'Foo'
        });

        expect(results[1].facts[0]).to.deep.equal({
          variable: 72,
          type: variable.types.categorical,
          attribute: 76
        });

        expect(results[1].facts[1]).to.deep.equal({
          variable: 98,
          type: variable.types.categorical,
          attribute: 5
        });

        expect(results[1].facts[2]).to.deep.equal({
          variable: 600,
          type: variable.types.numerical,
          value: 0.7
        });

        expect(results[1].facts[3]).to.deep.equal({
          variable: 700,
          type: variable.types.text,
          value: 'Bar'
        });

        done();
      });

    rawToIndividuals(56, schema).map(s => dataStream.pipe(s))
      .fork(error => {
        done(new Error(error));
      },
      results => {
        results.pipe(outStream);
      });
  });

  it('should use keys for variables and attributes', function(done) {

    const schema = [
      {
        variable: 73,
        attributes: [ 77, 78 ]
      }
    ];

    const entityRepository = entityRepoStub(commonTestEntities);

    const {rawToIndividuals} = individualGeneratorFac({entityRepository});

    const dataStream = StreamTest['v2'].fromObjects([{
      loc: 'MA-Keyed'
    }, {
      loc: 'NY-Keyed'
    }]);

    const outStream = StreamTest['v2'].toObjects(
      function(error, results) {
        expect(error).to.be.null;
        expect(results).to.have.length(2);

        expect(results[0].id).to.equal(1);
        expect(results[0].dataSet).to.equal(56);
        expect(results[0].facts).to.have.length(1);

        expect(results[0].facts[0]).to.deep.equal({
          variable: 73,
          type: variable.types.categorical,
          attribute: 77
        });

        expect(results[1].facts[0]).to.deep.equal({
          variable: 73,
          type: variable.types.categorical,
          attribute: 78
        });

        done();
      });

    rawToIndividuals(56, schema).map(s => dataStream.pipe(s))
      .fork(error => {
        done(new Error(error));
      },
      results => {
        results.pipe(outStream);
      });

  });

  it('should throw error for non matching attribute', function(done) {

    const schema = [
      {
        variable: 72,
        attributes: [ 45 ]
      }
    ];

    const entityRepository = entityRepoStub(commonTestEntities);
    const {rawToIndividuals} = individualGeneratorFac({entityRepository});

    const dataStream = StreamTest['v2'].fromObjects([{
      Location: 'UNKNOWN'
    }]);

    const outStream = StreamTest['v2'].toObjects(
      function() {
        //nop
        assert.fail('Stream process should not have completed successfully');
        done();
      });

    const streamTask = rawToIndividuals(56, schema)
      .map(s => dataStream.pipe(s))
      .map(stream => stream.on('error', function(err){
        expect(err).to.match(/Row 1/);
        expect(err).to.match(/Invalid mapping for attribute: Location: UNKNOWN/);
        done();
      }));

    streamTask
      .fork(error => {
        done(new Error(error));
      },
      results => {
        results.pipe(outStream);
      });
  });

  it('should allow blank attributes', function(done) {

    const schema = [
      {
        variable: 73,
        attributes: [ 77, 78 ]
      }
    ];

    const entityRepository = entityRepoStub(commonTestEntities);
    const {rawToIndividuals} = individualGeneratorFac({entityRepository});

    const dataStream = StreamTest['v2'].fromObjects([{
      loc: ''
    }, {
      loc: 'NY-Keyed'
    }]);

    const outStream = StreamTest['v2'].toObjects(
      function(error, results) {
        expect(error).to.be.null;
        expect(results).to.have.length(2);

        expect(results[0].id).to.equal(1);
        expect(results[0].dataSet).to.equal(56);
        expect(results[0].facts).to.have.length(1);

        expect(results[0].facts[0]).to.deep.equal({
          variable: 73,
          type: variable.types.categorical,
          attribute: null
        });

        expect(results[1].facts[0]).to.deep.equal({
          variable: 73,
          type: variable.types.categorical,
          attribute: 78
        });

        done();
      });

    rawToIndividuals(56, schema).map(s => dataStream.pipe(s))
      .fork(error => {
        done(new Error(error));
      },
      results => {
        results.pipe(outStream);
      });
  });

  it('should throw error for no schema present', function(done){

    const entityRepository = entityRepoStub(commonTestEntities);
    const {rawToIndividuals} = individualGeneratorFac({entityRepository});

    const dataStream = StreamTest['v2'].fromObjects([{
      Location: 'MA',
      Year: '2012',
      Percent: .5
    }, {
      Location: 'NY',
      Year: '2013',
      Percent: .7
    }]);

    rawToIndividuals(56).map(s => dataStream.pipe(s))
      .fork(error => {
        expect(error).to.match(/Invalid Schema/);
        done();
      },
      () => {
        done(new Error('Expected an error case, but got success'));
      });
  });

  it('should throw error for attribute that does not belong to the variable specified in the schema', function(done) {

    const schema = [
      {
        variable: 72,
        attributes: [ 4 ] // Doesn't belong to variable 72
      },
      {
        variable: 98,
        attributes: [ 77 ] // Doesn't belong to variable 98
      }
    ];

    const entityRepository = entityRepoStub(commonTestEntities);
    const {rawToIndividuals} = individualGeneratorFac({entityRepository});

    const dataStream = StreamTest['v2'].fromObjects([{
      Location: '2012'
    }, {
      Year: 'MA'
    }]);

    rawToIndividuals(56, schema)
      .map(s => dataStream.pipe(s))
      .fork(error => {
        expect(error).to.match(/Invalid Schema. The schema has mismatched variable\/attribute pairs: 72\/4, 98\/77/);
        done();
      },
      () => {
        done(new Error('Expected an error case, but got success'));
      });
  });

  it('should ignore columns that are not in the schema', function(done) {

    const schema = [
      {
        variable: 72,
        attributes: [ 45 ]
      }
    ];

    const entityRepository = entityRepoStub(commonTestEntities);
    const {rawToIndividuals} = individualGeneratorFac({entityRepository});

    const dataStream = StreamTest['v2'].fromObjects([{
      Ignored0: 'Ignored stuff',
      Location: 'MA',
      Ignored1: 'More Ignored Stuff'
    }]);

    const outStream = StreamTest['v2'].toObjects(
      function(error, results) {
        expect(error).to.be.null;
        expect(results).to.have.length(1);

        expect(results[0].id).to.equal(1);
        expect(results[0].dataSet).to.equal(56);
        expect(results[0].facts).to.have.length(1);

        expect(results[0].facts[0]).to.deep.equal({
          variable: 72,
          type: variable.types.categorical,
          attribute: 45
        });

        done();
      });

    rawToIndividuals(56, schema).map(s => dataStream.pipe(s))
      .fork(error => {
        done(new Error(error));
      },
      results => {
        results.pipe(outStream);
      });
  });

  it('should throw error for non number numerical variable', function(done) {

    const schema = [
      {
        variable: 600
      }
    ];

    const entityRepository = entityRepoStub(commonTestEntities);
    const {rawToIndividuals} = individualGeneratorFac({entityRepository});

    const dataStream = StreamTest['v2'].fromObjects([{
      Percent: 'Non-number'
    }]);

    const outStream = StreamTest['v2'].toObjects(
      function() {
        //nop
        assert.fail('Stream process should not have completed successfully');
        done();
      });

    const streamTask = rawToIndividuals(56, schema)
      .map(s => dataStream.pipe(s))
      .map(stream => stream.on('error', function(err){
        expect(err).to.match(/Row 1/);
        expect(err).to.match(/Non-numerical value for numerical variable: Percent: Non-number/);
        done();
      }));

    streamTask
      .fork(error => {
        done(new Error(error));
      },
      results => {
        results.pipe(outStream);
      });
  });

  it('should allow nulls', function(done) {

    const schema = [
      {
        variable: 72,
        attributes: [ 45 ]
      },
      {
        variable: 98,
        attributes: [ 4 ]
      },
      {
        variable: 600
      },
      {
        variable: 700
      }
    ];

    const entityRepository = entityRepoStub(commonTestEntities);
    const {rawToIndividuals} = individualGeneratorFac({entityRepository});

    const dataStream = StreamTest['v2'].fromObjects([{
      Location: 'MA',
      Year: null,
      Percent: null,
      Name: null
    }]);

    const outStream = StreamTest['v2'].toObjects(
      function(error, results) {
        expect(error).to.be.null;
        expect(results).to.have.length(1);

        expect(results[0].id).to.equal(1);
        expect(results[0].dataSet).to.equal(56);
        expect(results[0].facts).to.have.length(4);

        expect(results[0].facts[1].attribute).to.be.null;
        expect(results[0].facts[2].value).to.be.null;
        expect(results[0].facts[3].value).to.be.null;

        done();
      });

    rawToIndividuals(56, schema).map(s => dataStream.pipe(s))
      .fork(error => {
        done(new Error(error));
      },
      results => {
        results.pipe(outStream);
      });
  });

  it('should clean numerical values', function(done) {

    const schema = [
      {
        variable: 72,
        attributes: [ 45 ]
      },
      {
        variable: 600
      }
    ];

    const entityRepository = entityRepoStub(commonTestEntities);
    const {rawToIndividuals} = individualGeneratorFac({entityRepository});

    const dataStream = StreamTest['v2'].fromObjects([{
      Location: 'MA',
      Percent: '$56,000%'
    }]);

    const outStream = StreamTest['v2'].toObjects(
      function(error, results) {
        expect(error).to.be.null;
        expect(results).to.have.length(1);

        expect(results[0].facts[1].value).to.equal(56000);

        done();
      });

    rawToIndividuals(56, schema).map(s => dataStream.pipe(s))
      .fork(error => {
        done(new Error(error));
      },
      results => {
        results.pipe(outStream);
      });
  });
});
