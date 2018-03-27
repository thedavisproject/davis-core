const R = require('ramda');
const { thread } = require('davis-shared').fp;
const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
const sinonChai = require('sinon-chai');

chai.use(sinonChai);
chai.use(chaiAsPromised);

const {expect} = chai;

const Task = require('data.task');
const Async = require('control.async')(Task);
const when = require('when');

const sinon = require('sinon');

const task2Promise = Async.toPromise(when.promise);

const StreamTest = require('streamtest');
const {dataSet, individual, fact} = require('davis-model');

const importFac = require('../../../src/data/import/dataImport');

const testDataModifiedDate = new Date(2016,5,24,12,30,0,0);

const Transform = require('stream').Transform;

describe('Data Import', function(){

  const stubStorage = () => {

    const trxCommit = sinon.stub();
    const trxRollBack = sinon.stub();
    const trxStorage = {
      data: {
        create: sinon.stub(),
        delete: sinon.stub()
      },
      entities: {
        query: sinon.stub(),
        update: sinon.stub()
      }
    };

    const storageStub = {
      transact: sinon.stub().callsFake(fn =>
        new Task((reject, resolve) => {
          fn(trxStorage, err => {
            trxCommit();
            resolve(err);
          }, succ => {
            trxRollBack();
            reject(succ);
          });
        }))
    };

    const parseDataFileStub = sinon.stub();

    return {
      trxStorage,
      trxRollBack,
      trxCommit,
      storageStub,
      parseDataFileStub,
      config: { core: sinon.stub() }
    };
  };

  const stubIndividualGenerator = (expectedDataOutput, streamError) => {

    // Create toIndividuals stub and return that from the toIndividuals factory
    const toIndividualsProcessStub = sinon.stub();

    const toIndividualsStub = new Transform({
      objectMode: true,
      transform: (chunk, encoding, callback) => {
        if(streamError){
          callback(new Error(streamError), null);
          return;
        }
        callback(null, toIndividualsProcessStub(chunk));
      }
    });

    // On the first call (first item in the stream), return the expected individual
    for(let i = 0; i < expectedDataOutput.length; i++){
      toIndividualsProcessStub.onCall(i).returns(expectedDataOutput[i]);
    }

    const individualGeneratorStub = {
      rawToIndividuals: sinon.stub()
    };

    individualGeneratorStub.rawToIndividuals.returns(Task.of(toIndividualsStub));

    return individualGeneratorStub;
  };

  it('should write a single individual', function(){

    // Arrange
    const {
      trxRollBack,
      trxCommit,
      trxStorage,
      storageStub,
      parseDataFileStub,
      config} = stubStorage();

    const testSet = dataSet.new(2, 'Test Set');

    trxStorage.data.delete.returns(Task.of(true));
    trxStorage.data.create.returns(Task.of(1));
    trxStorage.entities.query
      .returns(Task.of([testSet]));
    trxStorage.entities.update
      .returns(Task.of([testSet]));

    // Stub the data stream
    const dataToImport = [
      individual.new(1, 1, [
        fact.newCategorical(9, 12),
        fact.newNumerical(10, 56),
        fact.newText(11, 'Foo')
      ])
    ];

    // Empty stream with one object
    parseDataFileStub.returns(StreamTest['v2'].fromObjects(dataToImport.map(() => ({}))));

    const individualGeneratorStub = stubIndividualGenerator(dataToImport);

    const importer = importFac({
      timeStamp: {
        now: sinon.stub().returns(testDataModifiedDate)
      },
      storage: storageStub,
      catalog: 'cat',
      individualGenerator: individualGeneratorStub,
      parseDataFile: parseDataFileStub,
      config
    });

    // Act
    const results = task2Promise(importer(2, {}, 'filepath', {
      batchSize: 1
    }));

    // Assert
    return when.all([
      expect(results).to.eventually.equal(1), // number of rows inserted
      results.then(() => expect(individualGeneratorStub.rawToIndividuals).to.have.been.calledWith(2)),
      results.then(() => expect(parseDataFileStub).to.have.been.calledWith('filepath')),
      results.then(() => expect(trxStorage.data.delete).to.have.been.calledWith('cat', {dataSet: 2})),
      results.then(() => expect(trxStorage.data.create).to.have.been.calledWith('cat', dataToImport)),
      results.then(() => expect(trxRollBack).to.not.have.been.called),
      results.then(() => expect(trxCommit).to.have.been.called)
    ]);
  });

  it('should update the data modified date', function(){

    // Arrange
    const {
      trxRollBack,
      trxCommit,
      trxStorage,
      storageStub,
      parseDataFileStub,
      config} = stubStorage();

    const testSet = dataSet.new(2, 'Test Set');

    trxStorage.data.delete.returns(Task.of(true));
    trxStorage.data.create.returns(Task.of(1));
    trxStorage.entities.query
      .returns(Task.of([testSet]));
    trxStorage.entities.update
      .returns(Task.of([testSet]));

    // Import no rows, jsut testing the data updated date
    const dataToImport = [ ];

    // Empty stream with one object
    parseDataFileStub.returns(StreamTest['v2'].fromObjects(dataToImport.map(() => ({}))));

    const individualGeneratorStub = stubIndividualGenerator(dataToImport);

    const importer = importFac({
      timeStamp: {
        now: sinon.stub().returns(testDataModifiedDate)
      },
      storage: storageStub,
      catalog: 'cat',
      individualGenerator: individualGeneratorStub,
      parseDataFile: parseDataFileStub,
      config
    });

    // Act
    const expectedUpdateItem = thread(
      testSet,
      R.assoc('schema', []),
      dataSet.setDataModified(testDataModifiedDate));

    const results = task2Promise(importer(2, {}, 'filepath', {
      batchSize: 1
    }));

    // Assert
    return when.all([
      expect(results).to.eventually.equal(0), // number of rows inserted
      results.then(() => expect(individualGeneratorStub.rawToIndividuals).to.have.been.calledWith(2)),
      results.then(() => expect(parseDataFileStub).to.have.been.calledWith('filepath')),
      results.then(() => expect(trxStorage.entities.update).to.have.been.calledWith('cat', [expectedUpdateItem])),
      results.then(() => expect(trxRollBack).to.not.have.been.called),
      results.then(() => expect(trxCommit).to.have.been.called)
    ]);

  });

  it('should set the dataset schema', function(){

    // Arrange
    const {
      trxRollBack,
      trxCommit,
      trxStorage,
      storageStub,
      parseDataFileStub,
      config} = stubStorage();

    const testSet = dataSet.new(2, 'Test Set');

    const schema = [
      { variable: 9, attributes: [12] },
      { variable: 10 },
      { variable: 11 }
    ];

    const columnMappings = {
      'foo': 5,
      'bar': 6
    };

    trxStorage.data.delete.returns(Task.of(true));
    trxStorage.data.create.returns(Task.of(1));
    trxStorage.entities.query
      .returns(Task.of([testSet]));
    trxStorage.entities.update
      .returns(Task.of([testSet]));

    const dataToImport = [
      individual.new(1, 1, [
        fact.newCategorical(9, 12),
        fact.newNumerical(10, 56),
        fact.newText(11, 'Foo')
      ])
    ];

    // Empty stream with one object
    parseDataFileStub.returns(StreamTest['v2'].fromObjects(dataToImport.map(() => ({}))));

    const individualGeneratorStub = stubIndividualGenerator(dataToImport);

    const importer = importFac({
      timeStamp: {
        now: sinon.stub().returns(testDataModifiedDate)
      },
      storage: storageStub,
      catalog: 'cat',
      individualGenerator: individualGeneratorStub,
      parseDataFile: parseDataFileStub,
      config
    });

    // Act
    const expectedUpdateItem = thread(
      testSet,
      R.assoc('schema', schema),
      dataSet.setDataModified(testDataModifiedDate));

    const results = task2Promise(importer(2, columnMappings, 'filepath', {
      batchSize: 1
    }));

    // Assert
    return when.all([
      expect(results).to.eventually.equal(1), // number of rows inserted
      results.then(() => expect(individualGeneratorStub.rawToIndividuals).to.have.been.calledWith(2, columnMappings)),
      results.then(() => expect(parseDataFileStub).to.have.been.calledWith('filepath')),
      results.then(() => expect(trxStorage.entities.update).to.have.been.calledWith('cat', [expectedUpdateItem])),
      results.then(() => expect(trxRollBack).to.not.have.been.called),
      results.then(() => expect(trxCommit).to.have.been.called)
    ]);

  });

  it('should roll back transaction on stream error', function(){

    // Arrange
    const {
      trxRollBack,
      trxCommit,
      trxStorage,
      storageStub,
      parseDataFileStub,
    config} = stubStorage();

    trxStorage.data.delete.returns(Task.of(true));
    trxStorage.data.create.returns(Task.of(1));

    const dataToImport = [
      individual.new(1, 1, [
        fact.newCategorical(9, 12),
        fact.newNumerical(10, 56)
      ])
    ];

    // Empty stream with one object
    parseDataFileStub.returns(StreamTest['v2'].fromObjects(dataToImport.map(() => ({}))));

    const individualGeneratorStub = stubIndividualGenerator(dataToImport, 'Stream Error');

    const importer = importFac({
      timeStamp: {
        now: sinon.stub().returns(testDataModifiedDate)
      },
      storage: storageStub,
      catalog: 'cat',
      individualGenerator: individualGeneratorStub,
      parseDataFile: parseDataFileStub,
      config
    });

    // Act
    const results = task2Promise(importer(2, {}, 'filename', {
      batchSize: 1
    }));

    // Assert
    return when.all([
      expect(results).to.be.rejectedWith(/Stream Error/),
      results.catch(() => expect(trxRollBack).to.have.been.called),
      results.catch(() => expect(trxCommit).to.not.have.been.called)
    ]);

  });

  it('should roll back transaction on data delete error', function(){

    // Arrange
    const {
      trxRollBack,
      trxCommit,
      trxStorage,
      storageStub,
      parseDataFileStub,
      config } = stubStorage();

    trxStorage.data.delete.returns(Task.rejected('Data delete error'));

    const dataToImport = [
      individual.new(1, 1, [
        fact.newCategorical(9, 12),
        fact.newNumerical(10, 56)
      ])
    ];

    // Empty stream with one object
    parseDataFileStub.returns(StreamTest['v2'].fromObjects(dataToImport.map(() => ({}))));

    const individualGeneratorStub = stubIndividualGenerator(dataToImport);

    const importer = importFac({
      timeStamp: {
        now: sinon.stub().returns(testDataModifiedDate)
      },
      storage: storageStub,
      catalog: 'cat',
      individualGenerator: individualGeneratorStub,
      parseDataFile: parseDataFileStub,
      config
    });

    // Act
    const results = task2Promise(importer(2, {}, 'filepath', {
      batchSize: 1
    }));

    // Assert
    return when.all([
      expect(results).to.be.rejectedWith(/Data delete error/),
      results.catch(() => expect(trxRollBack).to.have.been.called),
      results.catch(() => expect(trxCommit).to.not.have.been.called)
    ]);

  });

  it('should roll back transaction on data insert error', function(){

    // Arrange
    const {
      trxRollBack,
      trxCommit,
      trxStorage,
      storageStub,
      parseDataFileStub,
      config} = stubStorage();

    trxStorage.data.delete.returns(Task.of(true));
    trxStorage.data.create.returns(Task.rejected('Data write error'));

    const dataToImport = [
      individual.new(1, 1, [
        fact.newCategorical(9, 12),
        fact.newNumerical(10, 56)
      ])
    ];

    // Empty stream with one object
    parseDataFileStub.returns(StreamTest['v2'].fromObjects(dataToImport.map(() => ({}))));

    const individualGeneratorStub = stubIndividualGenerator(dataToImport);

    const importer = importFac({
      timeStamp: {
        now: sinon.stub().returns(testDataModifiedDate)
      },
      storage: storageStub,
      catalog: 'cat',
      individualGenerator: individualGeneratorStub,
      parseDataFile: parseDataFileStub,
      config
    });

    // Act
    const results = task2Promise(importer(2, {}, 'filepath', {
      batchSize: 1
    }));

    // Assert
    return when.all([
      expect(results).to.be.rejectedWith(/Data write error/),
      results.catch(() => expect(trxRollBack).to.have.been.called),
      results.catch(() => expect(trxCommit).to.not.have.been.called)
    ]);

  });

  it('should roll back transaction on entity read error', function(){

    // Arrange
    const {
      trxRollBack,
      trxCommit,
      trxStorage,
      storageStub,
      parseDataFileStub,
      config } = stubStorage();

    trxStorage.data.delete.returns(Task.of(true));
    trxStorage.data.create.returns(Task.of(1));
    trxStorage.entities.query
      .returns(Task.rejected('Entity read error'));

    const dataToImport = [
      individual.new(1, 1, [
        fact.newCategorical(9, 12),
        fact.newNumerical(10, 56)
      ])
    ];

    // Empty stream with one object
    parseDataFileStub.returns(StreamTest['v2'].fromObjects(dataToImport.map(() => ({}))));

    const individualGeneratorStub = stubIndividualGenerator(dataToImport);

    const importer = importFac({
      timeStamp: {
        now: sinon.stub().returns(testDataModifiedDate)
      },
      storage: storageStub,
      catalog: 'cat',
      individualGenerator: individualGeneratorStub,
      parseDataFile: parseDataFileStub,
      config
    });

    // Act
    const results = task2Promise(importer(2, {}, 'filepath', {
      batchSize: 1
    }));

    // Assert
    return when.all([
      expect(results).to.be.rejectedWith(/Entity read error/),
      results.catch(() => expect(trxRollBack).to.have.been.called),
      results.catch(() => expect(trxCommit).to.not.have.been.called)
    ]);

  });

  it('should roll back transaction on entity update error', function(){

    // Arrange
    const {
      trxRollBack,
      trxCommit,
      trxStorage,
      storageStub,
      parseDataFileStub,
      config } = stubStorage();

    const testSet = dataSet.new(2, 'Test Set');

    trxStorage.data.delete.returns(Task.of(true));
    trxStorage.data.create.returns(Task.of(1));
    trxStorage.entities.query
      .returns(Task.of([testSet]));
    trxStorage.entities.update
      .returns(Task.rejected('Entity update error'));

    const dataToImport = [
      individual.new(1, 1, [
        fact.newCategorical(9, 12),
        fact.newNumerical(10, 56)
      ])
    ];

    // Empty stream with one object
    parseDataFileStub.returns(StreamTest['v2'].fromObjects(dataToImport.map(() => ({}))));

    const individualGeneratorStub = stubIndividualGenerator(dataToImport);

    const importer = importFac({
      timeStamp: {
        now: sinon.stub().returns(testDataModifiedDate)
      },
      storage: storageStub,
      catalog: 'cat',
      individualGenerator: individualGeneratorStub,
      parseDataFile: parseDataFileStub,
      config
    });

    // Act
    const results = task2Promise(importer(2, {}, 'filepath', {
      batchSize: 1
    }));

    // Assert
    return when.all([
      expect(results).to.be.rejectedWith(/Entity update error/),
      results.catch(() => expect(trxRollBack).to.have.been.called),
      results.catch(() => expect(trxCommit).to.not.have.been.called)
    ]);

  });

  it('should write records when fewer than the batch size are created', function(){

    // Arrange
    const {
      trxRollBack,
      trxCommit,
      trxStorage,
      storageStub,
      parseDataFileStub,
      config } = stubStorage();

    const testSet = dataSet.new(2, 'Test Set');

    trxStorage.data.delete.returns(Task.of(true));
    trxStorage.data.create.returns(Task.of(2));
    trxStorage.entities.query
      .returns(Task.of([testSet]));
    trxStorage.entities.update
      .returns(Task.of([testSet]));

    const dataToImport = [
      individual.new(1, 1, [
        fact.newCategorical(9, 12),
        fact.newNumerical(10, 56)
      ]),
      individual.new(2, 1, [
        fact.newCategorical(9, 13),
        fact.newNumerical(10, 25)
      ])
    ];

    // Empty stream with one object
    parseDataFileStub.returns(StreamTest['v2'].fromObjects(dataToImport.map(() => ({}))));

    const individualGeneratorStub = stubIndividualGenerator(dataToImport);

    const importer = importFac({
      timeStamp: {
        now: sinon.stub().returns(testDataModifiedDate)
      },
      storage: storageStub,
      catalog: 'cat',
      individualGenerator: individualGeneratorStub,
      parseDataFile: parseDataFileStub,
      config
    });

    // Act
    const results = task2Promise(importer(2, {}, 'filepath', {
      batchSize: 1000
    }));
    // Assert
    return when.all([
      expect(results).to.eventually.equal(2), // number of rows inserted
      results.then(() => expect(trxStorage.data.create).to.have.been.calledWith('cat', dataToImport)),
      results.then(() => expect(trxRollBack).to.not.have.been.called),
      results.then(() => expect(trxCommit).to.have.been.called)
    ]);
  });

  it('should use the config timeout', function(){

    // Arrange
    const {
      trxStorage,
      storageStub,
      parseDataFileStub,
      config } = stubStorage();

    trxStorage.data.delete.returns(Task.of(true));
    trxStorage.data.create.returns(Task.of(2));
    trxStorage.entities.query
      .returns(Task.of([]));
    trxStorage.entities.update
      .returns(Task.of([]));

    const dataToImport = [];

    // Empty stream with one object
    parseDataFileStub.returns(StreamTest['v2'].fromObjects(dataToImport.map(() => ({}))));

    const individualGeneratorStub = stubIndividualGenerator(dataToImport);

    const importer = importFac({
      timeStamp: {
        now: sinon.stub().returns(testDataModifiedDate)
      },
      storage: storageStub,
      catalog: 'cat',
      individualGenerator: individualGeneratorStub,
      parseDataFile: parseDataFileStub,
      config
    });

    config.core['import-timeout'] = 50;

    // Act
    const resultsIgnored = task2Promise(importer(2, {}, 'filepath', {
      batchSize: 1000
    }));

    // Assert
    return when.all([
      expect(storageStub.transact).to.have.been.calledWith(sinon.match.any, 50)
    ]);
  });
});
