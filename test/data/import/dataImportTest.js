const R = require('ramda');
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

describe('Data Import', function(){

  const stubStorage = () => {

    const trxCommit = sinon.stub();
    const trxRollBack = sinon.stub();
    const trxStorage = {
      data: {
        create: sinon.stub()
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

    return {
      trxStorage,
      trxRollBack,
      trxCommit,
      storageStub
    };
  };

  it('should write a single individual', function(){

    // Arrange
    const {trxRollBack, trxCommit, trxStorage, storageStub} = stubStorage();

    const testSet = dataSet.new(2, 'Test Set');

    trxStorage.data.create.returns(Task.of(1));
    trxStorage.entities.query
      .returns(Task.of([testSet]));
    trxStorage.entities.update
      .returns(Task.of([testSet]));

    const importer = importFac({
      timeStamp: {
        now: sinon.stub().returns(testDataModifiedDate)
      },
      storage: storageStub,
      catalog: 'cat'
    });

    const dataToImport = [
      individual.new(1, 1, [
        fact.newCategorical(9, 12),
        fact.newQuantitative(10, 56)
      ])
    ];

    const inputStream = StreamTest['v2'].fromObjects(R.clone(dataToImport));

    // Act
    const results = task2Promise(importer(2, inputStream, 1));
    // Assert
    return when.all([
      expect(results).to.eventually.equal(1), // number of rows inserted
      results.then(() => expect(trxStorage.data.create).to.have.been.calledWith('cat', dataToImport)),
      results.then(() => expect(trxRollBack).to.not.have.been.called),
      results.then(() => expect(trxCommit).to.have.been.called)
    ]);
  });

  it('should update the data modified date', function(){

    // Arrange
    const {trxRollBack, trxCommit, trxStorage, storageStub} = stubStorage();

    const testSet = dataSet.new(2, 'Test Set');

    trxStorage.data.create.returns(Task.of(1));
    trxStorage.entities.query
      .returns(Task.of([testSet]));
    trxStorage.entities.update
      .returns(Task.of([testSet]));

    const importer = importFac({
      timeStamp: {
        now: sinon.stub().returns(testDataModifiedDate)
      },
      storage: storageStub,
      catalog: 'cat'
    });

    const dataToImport = [
      individual.new(1, 1, [
        fact.newCategorical(9, 12),  // John
        fact.newQuantitative(10, 56) // 56 years old
      ])
    ];

    const inputStream = StreamTest['v2'].fromObjects(dataToImport);

    // Act
    const expectedUpdateItem = dataSet.setDataModified(testDataModifiedDate, testSet);

    const results = task2Promise(importer(2, inputStream, 1));

    // Assert
    return when.all([
      expect(results).to.eventually.equal(1), // number of rows inserted
      results.then(() => expect(trxStorage.entities.update).to.have.been.calledWith('cat', [expectedUpdateItem])),
      results.then(() => expect(trxRollBack).to.not.have.been.called),
      results.then(() => expect(trxCommit).to.have.been.called)
    ]);

  });

  it('should roll back transaction on stream error', function(){

    // Arrange
    const {trxRollBack, trxCommit, trxStorage, storageStub} = stubStorage();

    trxStorage.data.create.returns(Task.of(1));

    const importer = importFac({
      timeStamp: {
        now: sinon.stub().returns(testDataModifiedDate)
      },
      storage: storageStub,
      catalog: 'cat'
    });

    const dataToImport = [
      individual.new(1, 1, [
        fact.newCategorical(9, 12),
        fact.newQuantitative(10, 56)
      ])
    ];

    const inputStream = StreamTest['v2'].fromErroredObjects('Stream Error', dataToImport);

    // Act
    const results = task2Promise(importer(2, inputStream, 1));

    // Assert
    return when.all([
      expect(results).to.be.rejectedWith(/Stream Error/),
      results.catch(() => expect(trxRollBack).to.have.been.called),
      results.catch(() => expect(trxCommit).to.not.have.been.called)
    ]);

  });

  it('should roll back transaction on data insert error', function(){

    // Arrange
    const {trxRollBack, trxCommit, trxStorage, storageStub} = stubStorage();

    trxStorage.data.create.returns(Task.rejected('Data write error'));

    const importer = importFac({
      timeStamp: {
        now: sinon.stub().returns(testDataModifiedDate)
      },
      storage: storageStub,
      catalog: 'cat'
    });

    const dataToImport = [
      individual.new(1, 1, [
        fact.newCategorical(9, 12),
        fact.newQuantitative(10, 56)
      ])
    ];

    const inputStream = StreamTest['v2'].fromObjects(dataToImport);

    // Act
    const results = task2Promise(importer(2, inputStream, 1));

    // Assert
    return when.all([
      expect(results).to.be.rejectedWith(/Data write error/),
      results.catch(() => expect(trxRollBack).to.have.been.called),
      results.catch(() => expect(trxCommit).to.not.have.been.called)
    ]);

  });

  it('should roll back transaction on entity read error', function(){

    // Arrange
    const {trxRollBack, trxCommit, trxStorage, storageStub} = stubStorage();

    trxStorage.data.create.returns(Task.of(1));
    trxStorage.entities.query
      .returns(Task.rejected('Entity read error'));

    const importer = importFac({
      timeStamp: {
        now: sinon.stub().returns(testDataModifiedDate)
      },
      storage: storageStub,
      catalog: 'cat'
    });

    const dataToImport = [
      individual.new(1, 1, [
        fact.newCategorical(9, 12),
        fact.newQuantitative(10, 56)
      ])
    ];

    const inputStream = StreamTest['v2'].fromObjects(dataToImport);

    // Act
    const results = task2Promise(importer(2, inputStream, 1));

    // Assert
    return when.all([
      expect(results).to.be.rejectedWith(/Entity read error/),
      results.catch(() => expect(trxRollBack).to.have.been.called),
      results.catch(() => expect(trxCommit).to.not.have.been.called)
    ]);

  });

  it('should roll back transaction on entity update error', function(){

    // Arrange
    const {trxRollBack, trxCommit, trxStorage, storageStub} = stubStorage();

    const testSet = dataSet.new(2, 'Test Set');

    trxStorage.data.create.returns(Task.of(1));
    trxStorage.entities.query
      .returns(Task.of([testSet]));
    trxStorage.entities.update
      .returns(Task.rejected('Entity update error'));

    const importer = importFac({
      timeStamp: {
        now: sinon.stub().returns(testDataModifiedDate)
      },
      storage: storageStub,
      catalog: 'cat'
    });

    const dataToImport = [
      individual.new(1, 1, [
        fact.newCategorical(9, 12),
        fact.newQuantitative(10, 56)
      ])
    ];

    const inputStream = StreamTest['v2'].fromObjects(dataToImport);

    // Act
    const results = task2Promise(importer(2, inputStream, 1));

    // Assert
    return when.all([
      expect(results).to.be.rejectedWith(/Entity update error/),
      results.catch(() => expect(trxRollBack).to.have.been.called),
      results.catch(() => expect(trxCommit).to.not.have.been.called)
    ]);

  });

  it('should write records when fewer than the batch size are created', function(){

    // Arrange
    const {trxRollBack, trxCommit, trxStorage, storageStub} = stubStorage();

    const testSet = dataSet.new(2, 'Test Set');

    trxStorage.data.create.returns(Task.of(2));
    trxStorage.entities.query
      .returns(Task.of([testSet]));
    trxStorage.entities.update
      .returns(Task.of([testSet]));

    const importer = importFac({
      timeStamp: {
        now: sinon.stub().returns(testDataModifiedDate)
      },
      storage: storageStub,
      catalog: 'cat'
    });

    const dataToImport = [
      individual.new(1, 1, [
        fact.newCategorical(9, 12),
        fact.newQuantitative(10, 56)
      ]),
      individual.new(2, 1, [
        fact.newCategorical(9, 13),
        fact.newQuantitative(10, 25)
      ])
    ];

    const inputStream = StreamTest['v2'].fromObjects(R.clone(dataToImport));

    // Act
    const results = task2Promise(importer(2, inputStream, 1000));
    // Assert
    return when.all([
      expect(results).to.eventually.equal(2), // number of rows inserted
      results.then(() => expect(trxStorage.data.create).to.have.been.calledWith('cat', dataToImport)),
      results.then(() => expect(trxRollBack).to.not.have.been.called),
      results.then(() => expect(trxCommit).to.have.been.called)
    ]);
  });
});
