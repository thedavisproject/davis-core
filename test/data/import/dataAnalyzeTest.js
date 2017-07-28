const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');

chai.use(chaiAsPromised);

const {expect} = chai;

const Task = require('data.task');
const Async = require('control.async')(Task);
const when = require('when');
const task2Promise = Async.toPromise(when.promise);

const StreamTest = require('streamtest');
const analyzeFac = require('../../../src/data/import/dataAnalyze');
const { variable, attribute } = require('davis-model');
const entityRepoStub = require('../../stub/entityRepositoryStub');

describe('Analyze', function(){

  const testEntities = [
    variable.newCategorical(45, 'Location'),
    variable.newCategorical(46, 'Location-Keyed', { key: 'loc'}),
    variable.newCategorical(67, 'Year'),
    variable.newCategorical(68, 'Year-Dupe'),
    variable.newQuantitative(32, 'Percent', { scopedDataSet: 56 }),
    variable.newCategorical(34, 'Make'),  // Global
    variable.newCategorical(35, 'Make', { scopedDataSet: 48 }), // Local
    attribute.new(23, 'MA', 45),
    attribute.new(24, 'NY', 45),
    attribute.new(25, 'MA', 46, { key: 'MA-Key' }),
    attribute.new(26, 'NY', 46, { key: 'NY-Key' }),
    attribute.new(56, '2012', 67, { key: '2012' }),
    attribute.new(57, '2012', 68, { key: '2012' })];

  it('should match all variables', function(){

    const entityRepository = entityRepoStub(testEntities);
    const analyze = analyzeFac({entityRepository});

    const dataStream = StreamTest['v2'].fromObjects([
      {'Location': 'MA', 'Year': '2012', 'Percent': '.5'},
      {'Location': 'NY', 'Year': '2012', 'Percent': '.7'}
    ]);

    const results = task2Promise(analyze(56, dataStream));

    return when.all([
      expect(results.then(r => r[0])).to.eventually.contain({
        key:      'Location',
        match:    true,
        variable: 45,
        scope:    'global',
        type:     'categorical'
      }),
      expect(results.then(r => r[1])).to.eventually.contain({
        key:      'Year',
        match:    true,
        variable: 67,
        scope:    'global',
        type:     'categorical'
      }),
      expect(results.then(r => r[2])).to.eventually.contain({
        key:      'Percent',
        match:    true,
        variable: 32,
        scope:    'local',
        type:     'quantitative'
      })
    ]);
  });

  it('should not match missing categorical variable', function(){
    const entityRepository = entityRepoStub(testEntities);
    const analyze = analyzeFac({entityRepository});

    const dataStream = StreamTest['v2'].fromObjects([
      {'Gender': 'Male'},
      {'Gender': 'Female'}
    ]);

    const results = task2Promise(analyze(56, dataStream));

    return when.all([
      expect(results.then(r => r[0])).to.eventually.contain({
        key: 'Gender',
        match: false
      })
    ]);
  });

  it('should choose local variables over global', function(){
    const entityRepository = entityRepoStub(testEntities);
    const analyze = analyzeFac({entityRepository});

    const dataStream = StreamTest['v2'].fromObjects([
      {'Make': 'Foo'},
      {'Make': 'Bar'}
    ]);
    const results = task2Promise(analyze(48, dataStream));

    return when.all([
      expect(results.then(r => r[0])).to.eventually.contain({
        key: 'Make',
        match: true,
        variable: 35,
        scope: 'local',
        type: 'categorical'
      })
    ]);
  });

  it('should ignore local variables for other data sets', function(){
    const entityRepository = entityRepoStub(testEntities);
    const analyze = analyzeFac({entityRepository});

    const dataStream = StreamTest['v2'].fromObjects([
      {'Make': 'Foo'},
      {'Make': 'Bar'}
    ]);
    const results = task2Promise(analyze(70, dataStream));

    return when.all([
      expect(results.then(r => r[0])).to.eventually.contain({
        key: 'Make',
        match: true,
        variable: 34,
        scope: 'global',
        type: 'categorical'
      })
    ]);
  });

  it('should include attributes for categorical variables', function(){
    const entityRepository = entityRepoStub(testEntities);
    const analyze = analyzeFac({entityRepository});

    const dataStream = StreamTest['v2'].fromObjects([
      {'Location': 'MA'},
      {'Location': 'NY'}
    ]);
    const results = task2Promise(analyze(56, dataStream));

    return when.all([
      expect(results.then(r => r[0].attributes[0])).to.eventually.contain({
        key: 'MA',
        match: true,
        attribute: 23
      }),
      expect(results.then(r => r[0].attributes[1])).to.eventually.contain({
        key: 'NY',
        match: true,
        attribute: 24
      })
    ]);
  });

  it('should mark missing attributes', function(){
    const entityRepository = entityRepoStub(testEntities);
    const analyze = analyzeFac({entityRepository});

    const dataStream = StreamTest['v2'].fromObjects([
      {'Location': 'MA'},
      {'Location': 'NY'},
      {'Location': 'VT'}
    ]);
    const results = task2Promise(analyze(56, dataStream));

    return when.all([
      expect(results.then(r => r[0].attributes[0].match)).to.eventually.be.true,
      expect(results.then(r => r[0].attributes[1].match)).to.eventually.be.true,
      expect(results.then(r => r[0].attributes[2].match)).to.eventually.be.false
    ]);
  });

  it('should skip empty attributes', function(){
    const entityRepository = entityRepoStub(testEntities);
    const analyze = analyzeFac({entityRepository});

    const dataStream = StreamTest['v2'].fromObjects([
      {'Location': 'MA'},
      {'Location': ''},
      {'Location': 'VT'}
    ]);
    const results = task2Promise(analyze(56, dataStream));
    return expect(results.then(r => r[0].attributes)).to.eventually.have.length(2);
  });

  it('should exclude attributes for quantitative variables', function(){
    const entityRepository = entityRepoStub(testEntities);
    const analyze = analyzeFac({entityRepository});

    const dataStream = StreamTest['v2'].fromObjects([
      {'Location': 'MA', 'Percent': .5},
      {'Location': 'NY', 'Percent': .7}
    ]);
    const results = task2Promise(analyze(56, dataStream));
    return expect(results.then(r => r[1].attributes)).to.eventually.not.exist;
  });

  it('should match variables by key', function(){
    const entityRepository = entityRepoStub(testEntities);
    const analyze = analyzeFac({entityRepository});

    const dataStream = StreamTest['v2'].fromObjects([
      {'loc': 'MA', 'Percent': '.5'},
      {'loc': 'NY', 'Percent': '.7'}
    ]);
    const results = task2Promise(analyze(56, dataStream));

    return when.all([
      expect(results.then(r => r[0])).to.eventually.contain({
        key: 'loc',
        match: true,
        variable: 46,
        scope: 'global',
        type: 'categorical'
      })
    ]);
  });

  it('should match attributes by key', function(){
    const entityRepository = entityRepoStub(testEntities);
    const analyze = analyzeFac({entityRepository});

    const dataStream = StreamTest['v2'].fromObjects([
      {'loc': 'MA-Key', 'Percent': '.5'},
      {'loc': 'NY-Key', 'Percent': '.7'}
    ]);
    const results = task2Promise(analyze(56, dataStream));

    return when.all([
      expect(results.then(r => r[0].attributes[0].match)).to.eventually.be.true,
      expect(results.then(r => r[0].attributes[1].match)).to.eventually.be.true
    ]);
  });

  it('should properly map duplicate named attributes', function(){
    const entityRepository = entityRepoStub(testEntities);
    const analyze = analyzeFac({entityRepository});

    const dataStream = StreamTest['v2'].fromObjects([
      {'Year': '2012', 'Year-Dupe': '2012'}
    ]);
    const results = task2Promise(analyze(56, dataStream));

    return when.all([
      expect(results.then(r => r[0])).to.eventually.contain({
        match: true,
        variable: 67
      }),
      expect(results.then(r => r[0].attributes[0])).to.eventually.contain({
        key: '2012',
        match: true,
        attribute: 56
      }),
      expect(results.then(r => r[1])).to.eventually.contain({
        match: true,
        variable: 68
      }),
      expect(results.then(r => r[1].attributes[0])).to.eventually.contain({
        key: '2012',
        match: true,
        attribute: 57
      })
    ]);
  });
});
