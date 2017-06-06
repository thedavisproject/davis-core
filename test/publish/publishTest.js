const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
const sinonChai = require('sinon-chai');

chai.use(sinonChai);
chai.use(chaiAsPromised);

const {expect} = chai;

const Task = require('data.task');
const Async = require('control.async')(Task);
const when = require('when');
const task2Promise = Async.toPromise(when.promise);

const sinon = require('sinon');

const model = require('davis-model');
const {entity, dataSet, action} = model;
const validEntityTypes = model.entityTypes;
const q = model.query.build;
const querySort = model.query.sort;
const publishFac = require('../../src/publish/publish');

describe('Publish', function(){

  // Set up for update methods
  const stubbIt = () => {
    const transactStub = sinon.stub();
    const storagePublishStub = {
      publishEntities: sinon.stub(),
      publishFacts: sinon.stub()
    };
    const entityRepoStub = {
      query: sinon.stub(),
      create: sinon.stub()
    };

    const trxStorage = {
      publish: storagePublishStub
    };

    transactStub.callsFake(fn =>
      fn(
        trxStorage,
        () => {throw Error('Commit should not be explicitly called');},
        () => {throw Error('Rollback should not be explicitly called');}));

    // ONLY provide transact. This will cause tests to fail if
    // any non transactional methods are called
    const storage = {
      transact: transactStub
    };

    const publish = publishFac({
      entityRepository: { transact: () => entityRepoStub },
      storage,
      catalog: 'source-cat',
      user: {id: 40}
    });

    return {
      storage,
      storagePublishStub,
      entityRepoStub,
      publish
    };
  };

  it('should wrap everything in a transaction and execute all steps', function(){

    const { storagePublishStub, entityRepoStub, publish } = stubbIt();

    entityRepoStub.query.withArgs(action.entityType)
      .returns(Task.of([])); // No action logs

    entityRepoStub.query.withArgs(dataSet.entityType)
      .returns(Task.of([ dataSet.new(5, 'my set') ]));

    entityRepoStub.create.returns(Task.of([]));

    storagePublishStub.publishEntities.returns(Task.of(true));
    storagePublishStub.publishFacts.returns(Task.of(true));

    const results = task2Promise(publish('target-cat'));

    return when.all([
      // Query for last full publish
      expect(entityRepoStub.query).to.have.been.calledWith(
        action.entityType,
        q.eq('action', action.actions.publish.full),
        {
          sort: querySort.desc('created'),
          take: 1
        }),
      // Query for all data sets since last full publish
      expect(entityRepoStub.query).to.have.been.calledWith(
        dataSet.entityType,
        q.gt('dataModified', new Date(0))
      ),
      // Add full publish action entry
      expect(entityRepoStub.create).to.have.been.calledWith(
        action.fullPublishEntry(40)),
      // Call publish entities with all entity types
      expect(storagePublishStub.publishEntities).to.have.been.calledWith(
        'source-cat',
        'target-cat',
        [...validEntityTypes]),
      // Call publish facts with the data sets that were changed
      expect(storagePublishStub.publishFacts).to.have.been.calledWith(
        'source-cat',
        'target-cat',
        [ 5 ]),
      expect(results).to.eventually.be.true
    ]);
  });

  it('should only publish data set data newer than last publish', function(){

    const { storagePublishStub, entityRepoStub, publish } = stubbIt();

    const lastPublishDate = new Date(2017,5,12,0,0,0);
    let lastPublish = action.fullPublishEntry(null);
    lastPublish = entity.setCreated(lastPublishDate, lastPublish);
    lastPublish = entity.setModified(lastPublishDate, lastPublish);

    entityRepoStub.query.withArgs(action.entityType)
      .returns(Task.of([lastPublish]));

    entityRepoStub.query.withArgs(dataSet.entityType)
      .returns(Task.of([]));

    entityRepoStub.create.returns(Task.of([]));

    storagePublishStub.publishEntities.returns(Task.of(true));
    storagePublishStub.publishFacts.returns(Task.of(true));

    const results = task2Promise(publish('target-cat'));

    return when.all([
      // Query for all data sets since last full publish
      expect(entityRepoStub.query).to.have.been.calledWith(
        dataSet.entityType,
        q.gt('dataModified', lastPublishDate)
      ),
      expect(results).to.eventually.be.true
    ]);
  });

  it('should publish all entity types', function(){

    const { storagePublishStub, entityRepoStub, publish } = stubbIt();

    entityRepoStub.query.withArgs(action.entityType)
      .returns(Task.of([])); // No action logs

    entityRepoStub.query.withArgs(dataSet.entityType)
      .returns(Task.of([]));

    entityRepoStub.create.returns(Task.of([]));

    storagePublishStub.publishEntities.returns(Task.of(true));
    storagePublishStub.publishFacts.returns(Task.of(true));

    const results = task2Promise(publish('target-cat'));

    return when.all([
      expect(storagePublishStub.publishEntities).to.have.been.calledWith(
        'source-cat',
        'target-cat',
        [...validEntityTypes]),
      expect(results).to.eventually.be.true
    ]);
  });

  it('should publish data for datasets that are returned by date query', function(){

    const { storagePublishStub, entityRepoStub, publish } = stubbIt();

    entityRepoStub.query.withArgs(action.entityType)
      .returns(Task.of([])); // No action logs

    entityRepoStub.query.withArgs(dataSet.entityType)
      .returns(Task.of([ dataSet.new(5, 'my set') ]));

    entityRepoStub.create.returns(Task.of([]));

    storagePublishStub.publishEntities.returns(Task.of(true));
    storagePublishStub.publishFacts.returns(Task.of(true));

    const results = task2Promise(publish('target-cat'));

    return when.all([
      expect(storagePublishStub.publishFacts).to.have.been.calledWith(
        'source-cat',
        'target-cat',
        [ 5 ]),
      expect(results).to.eventually.be.true
    ]);
  });

  it('should add a new publish action record', function(){

    const { storagePublishStub, entityRepoStub, publish } = stubbIt();

    entityRepoStub.query.withArgs(action.entityType)
      .returns(Task.of([])); // No action logs

    entityRepoStub.query.withArgs(dataSet.entityType)
      .returns(Task.of([]));

    entityRepoStub.create.returns(Task.of([]));

    storagePublishStub.publishEntities.returns(Task.of(true));
    storagePublishStub.publishFacts.returns(Task.of(true));

    const results = task2Promise(publish('target-cat'));

    return when.all([
      // Add full publish action entry
      expect(entityRepoStub.create).to.have.been.calledWith(
        action.fullPublishEntry(40)),
      expect(results).to.eventually.be.true
    ]);
  });

});
