const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
const sinonChai = require('sinon-chai');
const R = require('ramda');

chai.use(sinonChai);
chai.use(chaiAsPromised);

const {expect} = chai;

const Task = require('data.task');
const Async = require('control.async')(Task);
const when = require('when');
const task2Promise = Async.toPromise(when.promise);

const sinon = require('sinon');

const model = require('davis-model');
const {entity, dataSet, folder, variable, attribute, user, action} = model;
const q = model.query.build;

const {thread} = require('davis-shared').fp;

const entityRepositoryFac = require('../../src/entities/entityRepository');

const testCreatedDate = new Date(2016,5,24,12,30,0,0),
  testModifiedDate = new Date(2016,5,25,10,25,4,20);

describe('Entity Query', function(){

  // Set up for query methods
  const stubbIt = () => {
    const queryStub = sinon.stub();
    const storage = {
      entities: {
        query: queryStub
      }
    };
    const repo = entityRepositoryFac({
      timeStamp: {
        now: sinon.stub().returns(testCreatedDate)
      },
      storage,
      catalog: 'cat'
    });
    return {queryStub, storage, repo};
  };

  it('should bubble up Task errors from storage.entities.query for queryAll', function(){
    const {queryStub, repo} = stubbIt();
    queryStub.returns(Task.rejected('Error message'));
    const result = task2Promise(repo.queryAll(dataSet.entityType));
    return expect(result).to.be.rejectedWith('Error message');
  });

  it('should error bad entityType for queryAll', function(){
    const {repo} = stubbIt();
    const result = task2Promise(repo.queryAll('bad-type'));
    return expect(result).to.eventually.be.rejectedWith('Invalid entity type: bad-type');
  });

  it('should call storage.entities.query with no query expression for queryAll', function(){
    const {queryStub, repo} = stubbIt();

    const queryResults = [
      dataSet.new(5, 'Data Set')
    ];

    queryStub.returns(Task.of(queryResults));
    const result = task2Promise(repo.queryAll(dataSet.entityType));

    return when.all([
      expect(queryStub).to.have.been.calledWith('cat', dataSet.entityType),
      expect(result).to.eventually.deep.equal(queryResults)
    ]);
  });

  it('should bubble up Task errors from storage.entities.query for queryById', function(){
    const {queryStub, repo} = stubbIt();
    queryStub.returns(Task.rejected('Error message'));
    const result = task2Promise(repo.queryById(dataSet.entityType, [1]));
    return expect(result).to.eventually.be.rejectedWith('Error message');
  });

  it('should error bad entityType for queryById', function(){
    const {repo} = stubbIt();
    const result = task2Promise(repo.queryById('bad-type', [1,2,3]));
    return expect(result).to.eventually.be.rejectedWith('Invalid entity type: bad-type');
  });

  it('should call storage.entities.query with id in [] expression for queryById - multiple ids', function(){
    const {queryStub, repo} = stubbIt();

    const queryResults = [
      dataSet.new(5, 'Data Set 1'),
      dataSet.new(6, 'Data Set 2'),
      dataSet.new(7, 'Data Set 3')
    ];

    queryStub.returns(Task.of(queryResults));

    const result = task2Promise(repo.queryById(dataSet.entityType, [1,2,3]));

    return when.all([
      expect(queryStub).to.have.been.calledWith('cat', dataSet.entityType, ['in', 'id', [1,2,3]]),
      expect(result).to.eventually.deep.equal(queryResults)
    ]);
  });

  it('should call storage.entities.query with id in [] expression for queryById - single ids', function(){
    const {queryStub, repo} = stubbIt();

    const queryResults = [
      dataSet.new(5, 'Data Set')
    ];

    queryStub.returns(Task.of(queryResults));
    const result = task2Promise(repo.queryById(dataSet.entityType, 1));

    return when.all([
      expect(queryStub).to.have.been.calledWith('cat', dataSet.entityType, ['in', 'id', [1]]),
      expect(result).to.eventually.deep.equal(queryResults)
    ]);
  });

  it('should call storage.entities.query with id in [] expression for queryById - no id parameter', function(){
    const {queryStub, repo} = stubbIt();

    const queryResults = [
      dataSet.new(5, 'Data Set')
    ];

    queryStub.returns(Task.of(queryResults));
    const result = task2Promise(repo.queryById(dataSet.entityType));

    return when.all([
      expect(queryStub).to.have.been.calledWith('cat', dataSet.entityType, ['in', 'id', []]),
      expect(result).to.eventually.deep.equal(queryResults)
    ]);
  });

  it('should bubble up Task errors from storage.entities.query for query', function(){
    const {queryStub, repo} = stubbIt();
    queryStub.returns(Task.rejected('Error message'));
    const result = task2Promise(repo.query(dataSet.entityType, ['=', 'name', 'foo']));
    return expect(result).to.be.rejectedWith('Error message');
  });

  it('should error bad entityType for query', function(){
    const {repo} = stubbIt();
    const result = task2Promise(repo.query('bad-type', ['=', 'id', 5]));
    return expect(result).to.eventually.be.rejectedWith('Invalid entity type: bad-type');
  });

  it('should call storage.entities.query with id no expression for query with no expression', function(){
    const {queryStub, repo} = stubbIt();

    const queryResults = [
      dataSet.new(5, 'Data Set')
    ];

    queryStub.returns(Task.of(queryResults));
    const result = task2Promise(repo.query(dataSet.entityType));

    return when.all([
      expect(queryStub).to.have.been.calledWith('cat', dataSet.entityType, undefined),
      expect(result).to.eventually.deep.equal(queryResults)
    ]);
  });

  it('should call storage.entities.query with any query expression', function(){
    const {queryStub, repo} = stubbIt();

    const queryResults = [
      dataSet.new(5, 'Data Set')
    ];

    queryStub.returns(Task.of(queryResults));

    const result = task2Promise(repo.query(dataSet.entityType, ['some', 'expression']));

    return when.all([
      expect(queryStub).to.have.been.calledWith('cat', dataSet.entityType, ['some', 'expression']),
      expect(result).to.eventually.deep.equal(queryResults)
    ]);
  });

  it('should pass options through to the storage provider', function(){
    const {queryStub, repo} = stubbIt();

    const queryResults = [
      dataSet.new(5, 'Data Set')
    ];

    const options = { anything: 'foo' };

    queryStub.returns(Task.of(queryResults));

    const result = task2Promise(repo.query(dataSet.entityType, ['some', 'expression'], options));

    return when.all([
      expect(queryStub).to.have.been.calledWith('cat', dataSet.entityType, ['some', 'expression'], options),
      expect(result).to.eventually.deep.equal(queryResults)
    ]);
  });
});

describe('Entity Create', function(){

  // Set up for create methods
  const stubbIt = () => {

    const transactStub = sinon.stub();
    const createStub = sinon.stub();

    const trxStorage = {
      entities: {
        create: createStub
      }
    };

    transactStub.callsFake(fn =>
      fn(
        trxStorage,
        () => {throw Error('Commit should not be explicitly called');},
        () => {throw Error('Rollback should not be explicitly called');}));

    const storage = {
      transact: transactStub
    };

    const repo = entityRepositoryFac({
      timeStamp: {
        now: sinon.stub().returns(testCreatedDate)
      },
      storage,
      catalog: 'cat',
      user: {id: 25}
    });

    return {createStub, storage, repo};
  };

  it('should error for bad entity type', function(){
    const {repo} = stubbIt();

    const bogusEntity = entity.new('bad-type', null, 'Foo');
    const result = task2Promise(repo.create(bogusEntity));

    return expect(result).to.eventually.be.rejectedWith('Invalid entity type: bad-type');
  });

  it('should error for first bad entity type', function(){
    const {repo} = stubbIt();

    const bogusEntity1 = entity.new('bad-type1', null, 'Foo');
    const bogusEntity2 = entity.new('bad-type2', null, 'Foo');

    const result = task2Promise(repo.create([bogusEntity1, bogusEntity2]));

    return expect(result).to.eventually.be.rejectedWith('Invalid entity type: bad-type1');
  });

  it('should error if id is set', function(){
    const {repo} = stubbIt();

    const f = folder.new(567, 'Foo Folders');
    const result = task2Promise(repo.create(f));

    return expect(result).to.eventually.be.rejectedWith(/Entity objects must have empty id/);
  });

  it('should call storage.create with single entity', function(){
    const {createStub, repo} = stubbIt();

    const entity = dataSet.new(null, 'My Data Set');

    createStub.callsFake((catalog, entities) => Task.of(entities));

    const result = task2Promise(repo.create(entity));

    const entityPassedToCreate = createStub.args[0][1][0];

    return when.all([
      expect(createStub).to.have.been.calledWith('cat'),
      expect(entityPassedToCreate.name).to.equal('My Data Set'),
      // Make sure the entity is returned
      expect(result.then(R.head)).to.eventually.contain(entity)
    ]);
  });

  it('should call storage.create with multiple entities', function(){
    const {createStub, repo} = stubbIt();

    const entities = [
      dataSet.new(null, 'My Data Set'),
      folder.new(null, 'My Folder')
    ];

    createStub.callsFake((catalog, entities) => Task.of(entities));

    const resultIgnored = task2Promise(repo.create(entities));

    const entitiesPassedToCreate = createStub.args[0][1];

    return when.all([
      expect(entitiesPassedToCreate[0].id).to.equal(entities[0].id),
      expect(entitiesPassedToCreate[0].name).to.equal(entities[0].name),
      expect(entitiesPassedToCreate[1].id).to.equal(entities[1].id),
      expect(entitiesPassedToCreate[1].name).to.equal(entities[1].name),
      expect(createStub).to.have.been.calledWith('cat')
    ]);
  });

  it('should initialize the create and modified dates', function(){
    const {createStub, storage} = stubbIt();
    const repo = entityRepositoryFac({
      timeStamp: {
        now: sinon.stub().returns(testCreatedDate)
      },
      storage,
      catalog: 'cat',
      user: {id: 25}
    });

    const entity = dataSet.new(null, 'My Data Set');

    createStub.callsFake((catalog, entities) => Task.of(entities));

    const resultIgnored = task2Promise(repo.create(entity));

    const entityPassedToCreate = createStub.args[0][1][0];

    return when.all([
      expect(entityPassedToCreate.created.getTime()).to.equal(testCreatedDate.getTime()),
      expect(entityPassedToCreate.modified.getTime()).to.equal(testCreatedDate.getTime())
    ]);
  });

  it('should update the action log', function(){
    const {createStub, storage} = stubbIt();
    const repo = entityRepositoryFac({
      timeStamp: {
        now: sinon.stub().returns(testCreatedDate)
      },
      storage,
      catalog: 'cat',
      user: {id: 25}
    });

    const entities = [
      dataSet.new(null, 'My Data Set'),
      folder.new(null, 'My Folder')
    ];

    createStub.callsFake((catalog, entities) => Task.of(entities));

    const resultIgnored = task2Promise(repo.create(entities));

    // Second call (action log call)
    const entitiesPassedToCreate = createStub.args[1][1];

    const expectedActionLogEntries = entities.map(e => thread(
      action.entityCreatedEntry(25, e),
      entity.setCreated(testCreatedDate),
      entity.setModified(testCreatedDate)));

    return when.all([
      expect(entitiesPassedToCreate).to.deep.equal(expectedActionLogEntries)
    ]);
  });
});

describe('Entity Update', function(){

  // Set up for update methods
  const stubbIt = () => {

    const transactStub = sinon.stub();
    const createStub = sinon.stub();
    const updateStub = sinon.stub();

    const trxStorage = {
      entities: {
        create: createStub,
        update: updateStub
      }
    };

    transactStub.callsFake(fn =>
      fn(
        trxStorage,
        () => {throw Error('Commit should not be explicitly called');},
        () => {throw Error('Rollback should not be explicitly called');}));

    const storage = {
      transact: transactStub
    };

    const repo = entityRepositoryFac({
      timeStamp: {
        now: sinon.stub().returns(testCreatedDate)
      },
      storage,
      catalog: 'cat',
      user: {id: 25}
    });

    return {updateStub, createStub, storage, repo};
  };

  it('should error for bad entity type', function(){
    const {repo} = stubbIt();

    const bogusEntity = entity.new('bad-type', 2, 'Foo');
    const result = task2Promise(repo.update(bogusEntity));

    return expect(result).to.eventually.be.rejectedWith('Invalid entity type: bad-type');
  });

  it('should error if id is not set', function(){
    const {repo} = stubbIt();

    const f = folder.new(null, 'Foo Folders');
    const result = task2Promise(repo.update(f));

    return expect(result).to.eventually.be.rejectedWith(/Entity objects must not have empty id/);
  });

  it('should call storage.update with single entity', function(){
    const {updateStub, createStub, repo} = stubbIt();

    const entity = dataSet.new(2, 'Population - Updated');

    updateStub.callsFake((catalog, entities) => Task.of(entities));
    createStub.callsFake((catalog, entities) => Task.of(entities));

    const result = task2Promise(repo.update(entity));

    const entitiesPassedToUpdate = updateStub.args[0][1];

    return when.all([
      expect(entitiesPassedToUpdate[0].id).to.equal(entity.id),
      expect(entitiesPassedToUpdate[0].name).to.equal(entity.name),
      expect(updateStub).to.have.been.calledWith('cat'),
      expect(result.then(R.head)).to.eventually.contain(entity)
    ]);
  });

  it('should call storage.update with multiple entities', function(){
    const {updateStub, createStub, repo} = stubbIt();

    const entities = [
      dataSet.new(2, 'Population - Updated'),
      dataSet.new(5, 'Population 2 - Updated')
    ];

    updateStub.callsFake((catalog, entities) => Task.of(entities));
    createStub.callsFake((catalog, entities) => Task.of(entities));

    const resultIgnored = task2Promise(repo.update(entities));

    const entitiesPassedToUpdate = updateStub.args[0][1];

    return when.all([
      expect(entitiesPassedToUpdate[0].id).to.equal(entities[0].id),
      expect(entitiesPassedToUpdate[0].name).to.equal(entities[0].name),
      expect(entitiesPassedToUpdate[1].id).to.equal(entities[1].id),
      expect(entitiesPassedToUpdate[1].name).to.equal(entities[1].name),
      expect(updateStub).to.have.been.calledWith('cat')
    ]);
  });

  it('should call storage.update with multiple entities - different entity types', function(){
    const {updateStub, createStub, repo} = stubbIt();

    const entities = [
      dataSet.new(2, 'Population - Updated'),
      folder.new(5, 'Population 2 - Updated')
    ];

    updateStub.callsFake((catalog, entities) => Task.of(entities));
    createStub.callsFake((catalog, entities) => Task.of(entities));

    const resultIgnored = task2Promise(repo.update(entities));

    const entitiesPassedToUpdate = updateStub.args[0][1];

    return when.all([
      expect(entitiesPassedToUpdate[0].id).to.equal(entities[0].id),
      expect(entitiesPassedToUpdate[0].name).to.equal(entities[0].name),
      expect(entitiesPassedToUpdate[1].id).to.equal(entities[1].id),
      expect(entitiesPassedToUpdate[1].name).to.equal(entities[1].name),
      expect(updateStub).to.have.been.calledWith('cat')
    ]);
  });

  it('should update the modified dates', function(){
    const {updateStub, createStub, storage} = stubbIt();

    const repo = entityRepositoryFac({
      timeStamp: {
        now: sinon.stub().returns(testModifiedDate)
      },
      storage,
      catalog: 'cat',
      user: {id: 25}
    });

    const entity = attribute.new(6, 'Honda - New', 5);

    updateStub.callsFake((catalog, entities) => Task.of(entities));
    createStub.callsFake((catalog, entities) => Task.of(entities));

    const resultIgnored = task2Promise(repo.update([entity]));
    const entityPassedToUpdate = updateStub.args[0][1][0];

    expect(entityPassedToUpdate.modified.getTime()).to.equal(testModifiedDate.getTime());
  });

  it('should update the action log', function(){
    const {updateStub, createStub, storage} = stubbIt();

    const repo = entityRepositoryFac({
      timeStamp: {
        now: sinon.stub().returns(testCreatedDate)
      },
      storage,
      catalog: 'cat',
      user: {id: 25}
    });

    const entities = [
      dataSet.new(10, 'My Data Set'),
      folder.new(11, 'My Folder')
    ];

    createStub.callsFake((catalog, entities) => Task.of(entities));
    updateStub.callsFake((catalog, entities) => Task.of(entities));

    const resultIgnored = task2Promise(repo.update(entities));

    // First call (action log call)
    const entitiesPassedToCreate = createStub.args[0][1];

    const expectedActionLogEntries = entities.map(e => thread(
      action.entityUpdatedEntry(25, e),
      entity.setCreated(testCreatedDate),
      entity.setModified(testCreatedDate)));

    return when.all([
      expect(entitiesPassedToCreate).to.deep.equal(expectedActionLogEntries)
    ]);
  });
});

describe('Entity Delete', function(){

  // Set up for update methods
  const stubbIt = () => {
    const queryStub = sinon.stub();
    const createStub = sinon.stub();
    const deleteStub = sinon.stub();
    const transactStub = sinon.stub();
    const dataDelete = sinon.stub();

    const trxStorage = {
      entities: {
        create: createStub,
        query: queryStub,
        delete: deleteStub
      }
    };

    transactStub.callsFake(fn =>
      fn(
        trxStorage,
        () => {throw Error('Commit should not be explicitly called');},
        () => {throw Error('Rollback should not be explicitly called');}));

    const storage = {
      entities: {
        query: queryStub
      },
      transact: transactStub
    };

    const repo = entityRepositoryFac({
      timeStamp: {
        now: sinon.stub().returns(testCreatedDate)
      },
      dataDelete,
      storage,
      catalog: 'cat',
      user: {id: 25}
    });

    return {queryStub, createStub, deleteStub, transactStub, dataDelete, storage, repo};
  };

  it('should error for bad entity type', function(){
    const {repo} = stubbIt();

    const result = task2Promise(repo.delete('bad-type', 4));

    return expect(result).to.eventually.be.rejectedWith('Invalid entity type: bad-type');
  });

  it('should wrap delete in a transaction and call storage/delete (no dependents)', function(){
    const {deleteStub, createStub, transactStub, repo} = stubbIt();

    deleteStub.returns(Task.of(true));
    createStub.callsFake((catalog, entities) => Task.of(entities));

    const result = task2Promise(repo.delete(user.entityType, 1));

    return when.all([
      expect(transactStub).to.have.been.called,
      expect(result).to.eventually.be.true
    ]);
  });

  it('should query for and delete dependents', function(){
    const {queryStub, createStub, deleteStub, dataDelete, repo} = stubbIt();

    // Return an attribute dependent
    queryStub.withArgs(
      'cat',
      attribute.entityType,
      q.in('variable', [5]))
    .returns(Task.of([
      attribute.new(12, 'Foo', 5)]));

    // Return no dependents
    queryStub.withArgs(
      'cat',
      attribute.entityType,
      q.in('parent', [12]))
    .returns(Task.of([]));

    deleteStub.returns(Task.of(true));
    dataDelete.returns(Task.of(true));
    createStub.callsFake((catalog, entities) => Task.of(entities));

    const result = task2Promise(repo.delete(variable.entityType, 5));

    return when.all([
      expect(deleteStub).to.have.been.calledWith('cat', variable.entityType, [5]),
      expect(deleteStub).to.have.been.calledWith('cat', attribute.entityType, [12]),
      expect(result).to.eventually.be.true
    ]);
  });

  it('should delete dependents for folders', function(){
    const {queryStub, createStub, deleteStub, dataDelete, repo} = stubbIt();

    // Return a folder dependent
    queryStub.withArgs(
      'cat',
      folder.entityType,
      q.in('parent', [5]))
    .returns(Task.of([
      folder.new(12, 'Foo')]));

    // Return a data set dependent
    queryStub.withArgs(
      'cat',
      dataSet.entityType,
      q.in('folder', [5]))
    .returns(Task.of([
      dataSet.new(13, 'Bar')]));

    // Cut off dependent recursion (return no dependents
    // for next level dependent folders, variables, etc)
    queryStub.returns(Task.of([]));

    deleteStub.returns(Task.of(true));
    dataDelete.returns(Task.of(true));
    createStub.callsFake((catalog, entities) => Task.of(entities));

    const result = task2Promise(repo.delete(folder.entityType, 5));

    return when.all([
      expect(deleteStub).to.have.been.calledWith('cat', folder.entityType, [5]),
      expect(deleteStub).to.have.been.calledWith('cat', folder.entityType, [12]),
      expect(deleteStub).to.have.been.calledWith('cat', dataSet.entityType, [13]),
      expect(result).to.eventually.be.true
    ]);
  });

  it('should delete dependents for data sets', function(){
    const {queryStub, createStub, deleteStub, dataDelete, repo} = stubbIt();

    // Return a variable dependent
    queryStub.withArgs(
      'cat',
      variable.entityType,
      q.in('scopedDataSet', [5]))
    .returns(Task.of([
      variable.newCategorical(12, 'Foo', {scopedDataSet: 5})]));

    // Cut off dependent recursion (return no dependents
    // for next level dependent folders, variables, etc)
    queryStub.returns(Task.of([]));

    deleteStub.returns(Task.of(true));
    dataDelete.returns(Task.of(true));
    createStub.callsFake((catalog, entities) => Task.of(entities));

    const result = task2Promise(repo.delete(dataSet.entityType, 5));

    return when.all([
      expect(deleteStub).to.have.been.calledWith('cat', dataSet.entityType, [5]),
      expect(deleteStub).to.have.been.calledWith('cat', variable.entityType, [12]),
      expect(dataDelete).to.have.been.calledWith({ dataSet: [5]}),
      expect(result).to.eventually.be.true
    ]);
  });

  it('should delete dependents for variables', function(){
    const {queryStub, createStub, deleteStub, dataDelete, repo} = stubbIt();

    // Return a attribute dependent
    queryStub.withArgs(
      'cat',
      attribute.entityType,
      q.in('variable', [5]))
    .returns(Task.of([
      attribute.new(12, 'Foo', 5)]));

    // Cut off dependent recursion (return no dependents
    // for next level dependent folders, variables, etc)
    queryStub.returns(Task.of([]));

    deleteStub.returns(Task.of(true));
    dataDelete.returns(Task.of(true));
    createStub.callsFake((catalog, entities) => Task.of(entities));

    const result = task2Promise(repo.delete(variable.entityType, 5));

    return when.all([
      expect(deleteStub).to.have.been.calledWith('cat', variable.entityType, [5]),
      expect(deleteStub).to.have.been.calledWith('cat', attribute.entityType, [12]),
      expect(dataDelete).to.have.been.calledWith({ variable: [5] }),
      expect(result).to.eventually.be.true
    ]);
  });

  it('should delete dependents for attributes', function(){
    const {queryStub, createStub, deleteStub, dataDelete, repo} = stubbIt();

    // Return a attribute dependent
    queryStub.withArgs(
      'cat',
      attribute.entityType,
      q.in('parent', [5]))
    .returns(Task.of([
      attribute.new(12, 'Foo', 5)]));

    // Cut off dependent recursion (return no dependents
    // for next level dependent folders, variables, etc)
    queryStub.returns(Task.of([]));

    deleteStub.returns(Task.of(true));
    dataDelete.returns(Task.of(true));
    createStub.callsFake((catalog, entities) => Task.of(entities));

    const result = task2Promise(repo.delete(attribute.entityType, 5));

    return when.all([
      expect(deleteStub).to.have.been.calledWith('cat', attribute.entityType, [5]),
      expect(deleteStub).to.have.been.calledWith('cat', attribute.entityType, [12]),
      expect(dataDelete).to.have.been.calledWith({ attribute: [5] }),
      expect(result).to.eventually.be.true
    ]);
  });

  it('should call storage.delete with multiple entity ids', function(){
    const {queryStub, createStub, deleteStub, repo} = stubbIt();

    // No dependents
    queryStub.returns(Task.of([]));

    deleteStub.returns(Task.of(true));
    createStub.callsFake((catalog, entities) => Task.of(entities));

    const result = task2Promise(repo.delete(folder.entityType, [1, 4, 5]));

    return when.all([
      expect(deleteStub).to.have.been.calledWith('cat', folder.entityType, [1, 4, 5]),
      expect(result).to.eventually.be.true
    ]);
  });

  it('should update the action log', function(){
    const {deleteStub, queryStub, createStub, storage} = stubbIt();

    const repo = entityRepositoryFac({
      timeStamp: {
        now: sinon.stub().returns(testCreatedDate)
      },
      storage,
      catalog: 'cat',
      user: {id: 25}
    });

    // No dependents
    queryStub.returns(Task.of([]));

    createStub.callsFake((catalog, entities) => Task.of(entities));
    deleteStub.returns(Task.of(true));

    const resultIgnored = task2Promise(repo.delete(folder.entityType, [1, 4, 5]));

    // First call (action log call)
    const entitiesPassedToCreate = createStub.args[0][1];

    const expectedActionLogEntries = [1,4,5].map(e => thread(
      action.entityDeletedEntry(25, folder.entityType, e),
      entity.setCreated(testCreatedDate),
      entity.setModified(testCreatedDate)));

    return when.all([
      expect(entitiesPassedToCreate).to.deep.equal(expectedActionLogEntries)
    ]);
  });
});

describe('Entity Hierarchy', function(){

  // Set up for update methods
  const stubbIt = () => {
    const queryStub = sinon.stub();
    const storage = {
      entities: {
        query: queryStub
      }
    };
    const repo = entityRepositoryFac({
      timeStamp: {
        now: sinon.stub().returns(testCreatedDate)
      },
      storage,
      catalog: 'cat',
      user: {id: 25} 
    });
    return {queryStub, storage, repo};
  };

  it('should error for non hierarchy entity', function(){
    const {repo} = stubbIt();

    const entity = dataSet.new(null, 'Set');
    const result = task2Promise(repo.getParent(entity));

    return expect(result).to.eventually.be.rejectedWith(/not a hierarchical item/);
  });

  it('should get entitys parent if it exists', function(){
    const {queryStub, repo} = stubbIt();

    const childFolder = folder.new(2, 'My Folder', {parent: 6});

    const returned = folder.new(6, 'Parent');

    queryStub.returns(Task.of([returned]));

    const result = task2Promise(repo.getParent(childFolder));

    return when.all([
      expect(queryStub).to.have.been.calledWith('cat', folder.entityType, ['=', 'id', 6]),
      expect(result.then(r => r.isJust)).to.eventually.be.true,
      expect(result.then(r => r.get())).to.eventually.contain({
        id: 6,
        name: 'Parent'
      })
    ]);
  });

  it('should get Maybe.Nothing if entity has no parent id', function(){
    const {repo} = stubbIt();

    const childFolder = folder.new(2, 'My Folder', {parent: null});

    const result = task2Promise(repo.getParent(childFolder));

    return expect(result.then(r => r.isNothing)).to.eventually.be.true;
  });

  it('should get Maybe.Nothing if entity has parent id, but no parent is present', function(){
    const {queryStub, repo} = stubbIt();

    const childFolder = folder.new(2, 'My Folder', {parent: 6});

    queryStub.returns(Task.of([]));

    const result = task2Promise(repo.getParent(childFolder));

    return when.all([
      expect(queryStub).to.have.been.calledWith('cat', folder.entityType, ['=', 'id', 6]),
      expect(result.then(r => r.isNothing)).to.eventually.be.true
    ]);
  });

  it('should error for non hierarchy entity', function(){
    const {repo} = stubbIt();

    const entity = dataSet.new(null, 'Set');
    const result = task2Promise(repo.getChildren(entity));

    return expect(result).to.eventually.be.rejectedWith(/not a hierarchical item/);
  });

  it('should get empty array if entity has no children', function(){
    const {queryStub, repo} = stubbIt();

    const parentFolder = folder.new(2, 'My Folder');

    queryStub.returns(Task.of([]));

    const result = task2Promise(repo.getChildren(parentFolder));

    return when.all([
      expect(queryStub).to.have.been.calledWith('cat', folder.entityType, ['=', 'parent', 2]),
      expect(result).to.eventually.deep.equal([])
    ]);
  });

  it('should get all children', function(){
    const {queryStub, repo} = stubbIt();

    const parentFolder = folder.new(2, 'My Folder');
    const children = [
      folder.new(3, 'Child 1', {parent: 2}),
      folder.new(4, 'Child 2', {parent: 2})
    ];

    queryStub.returns(Task.of(children));

    const result = task2Promise(repo.getChildren(parentFolder));

    return when.all([
      expect(queryStub).to.have.been.calledWith('cat', folder.entityType, ['=', 'parent', 2]),
      expect(result).to.eventually.deep.equal(children)
    ]);
  });
});

describe('Entity Events', function(){

  describe('should emit before:create event for entities', function(){

    const transactStub = sinon.stub();
    const createStub = sinon.stub();

    const trxStorage = {
      entities: {
        create: createStub
      }
    };

    transactStub.callsFake(fn =>
      fn(
        trxStorage,
        () => {throw Error('Commit should not be explicitly called');},
        () => {throw Error('Rollback should not be explicitly called');}));

    const storage = {
      transact: transactStub
    };

    const repo = entityRepositoryFac({
      timeStamp: {
        now: sinon.stub().returns(testCreatedDate)
      },
      storage,
      catalog: 'cat',
      user: {id: 25}
    });

    createStub.callsFake((cat, entities) => Task.of(entities));

    function testEntity(e){
      const handler = sinon.stub();
      repo.events.on(`before:create:${e.entityType}`, handler);
      const result = task2Promise(repo.create(e));

      const entityPassedToHandler = handler.args[0][0];
      expect(entityPassedToHandler).to.contain(e);

      return result;
    }

    it(' -- Folders', function(){
      return testEntity(folder.new(null, 'Test Folder'));
    });
    it(' -- DataSets', function(){
      return testEntity(dataSet.new(null, 'Test DataSet'));
    });
    it(' -- Variables', function(){
      return testEntity(variable.newCategorical(null, 'Test Variable'));
    });
    it(' -- Attributes', function(){
      return testEntity(attribute.new(null, 'Test Attribute', 4));
    });
    it(' -- Users', function(){
      return testEntity(user.new(null, 'Test User'));
    });
    it(' -- Actions', function(){
      return testEntity(action.fullPublishEntry(null, 'Test Action'));
    });

    it('including multiple entities', function(){
      const testFolders = [
        folder.new(null, 'Folder One'),
        folder.new(null, 'Folder Two')
      ];

      const handler = sinon.stub();
      repo.events.on(`before:create:${folder.entityType}`, handler);
      const result = task2Promise(repo.create(testFolders));

      const handlerArgs = handler.args;
      // First call
      expect(handlerArgs[0][0]).to.contain(testFolders[0]);
      // Second call
      expect(handlerArgs[1][0]).to.contain(testFolders[1]);

      return result;
    });
  });

  describe('should emit after:create event for entities', function(){

    const transactStub = sinon.stub();
    const createStub = sinon.stub();

    const trxStorage = {
      entities: {
        create: createStub
      }
    };

    transactStub.callsFake(fn =>
      fn(
        trxStorage,
        () => {throw Error('Commit should not be explicitly called');},
        () => {throw Error('Rollback should not be explicitly called');}));

    const storage = {
      transact: transactStub
    };

    const repo = entityRepositoryFac({
      timeStamp: {
        now: sinon.stub().returns(testCreatedDate)
      },
      storage,
      catalog: 'cat',
      user: {id: 25}
    });

    createStub.callsFake((cat, entities) => Task.of(entities));

    function testEntity(e){
      const handler = sinon.stub();
      repo.events.on(`after:create:${e.entityType}`, handler);
      const result = task2Promise(repo.create(e));

      const entityPassedToHandler = handler.args[0][0];
      expect(entityPassedToHandler).to.contain(e);
      return result;
    }

    it(' -- Folders', function(){
      return testEntity(folder.new(null, 'Test Folder'));
    });
    it(' -- DataSets', function(){
      return testEntity(dataSet.new(null, 'Test DataSet'));
    });
    it(' -- Variables', function(){
      return testEntity(variable.newCategorical(null, 'Test Variable'));
    });
    it(' -- Attributes', function(){
      return testEntity(attribute.new(null, 'Test Attribute', 4));
    });
    it(' -- Users', function(){
      return testEntity(user.new(null, 'Test User'));
    });
    it(' -- Actions', function(){
      return testEntity(action.fullPublishEntry(null, 'Test Action'));
    });

    it('including multiple entities', function(){
      const testFolders = [
        folder.new(null, 'Folder One'),
        folder.new(null, 'Folder Two')
      ];

      const handler = sinon.stub();
      repo.events.on(`after:create:${folder.entityType}`, handler);
      const result = task2Promise(repo.create(testFolders));

      const handlerArgs = handler.args;
      // First call
      expect(handlerArgs[0][0]).to.contain(testFolders[0]);
      // Second call
      expect(handlerArgs[1][0]).to.contain(testFolders[1]);

      return result;
    });

  });

  it('should not emit create events (before and after) if the entity validation fails', function(){

    const transactStub = sinon.stub();
    const createStub = sinon.stub();

    const trxStorage = {
      entities: {
        create: createStub
      }
    };

    transactStub.callsFake(fn =>
      fn(
        trxStorage,
        () => {throw Error('Commit should not be explicitly called');},
        () => {throw Error('Rollback should not be explicitly called');}));

    const storage = {
      transact: transactStub
    };

    const repo = entityRepositoryFac({
      timeStamp: {
        now: sinon.stub().returns(testCreatedDate)
      },
      storage,
      catalog: 'cat',
      user: {id: 25}
    });

    createStub.callsFake((cat, entities) => Task.of(entities));

    // Giving this an ID will cause validation to fail
    const testFolder = folder.new(4, 'Folder One');

    const beforeHandler = sinon.stub();
    repo.events.on(`before:create:${folder.entityType}`, beforeHandler);
    const afterHandler = sinon.stub();
    repo.events.on(`after:create:${folder.entityType}`, afterHandler);

    const result = task2Promise(repo.create(testFolder));

    return when.all([
      expect(beforeHandler).to.not.be.called,
      expect(afterHandler).to.not.be.called,
      expect(result).to.be.rejected
    ]);
  });

  it('should not emit create events (after) if storage throws an error', function(){

    const transactStub = sinon.stub();
    const createStub = sinon.stub();

    const trxStorage = {
      entities: {
        create: createStub
      }
    };

    transactStub.callsFake(fn =>
      fn(
        trxStorage,
        () => {throw Error('Commit should not be explicitly called');},
        () => {throw Error('Rollback should not be explicitly called');}));

    const storage = {
      transact: transactStub
    };

    const repo = entityRepositoryFac({
      timeStamp: {
        now: sinon.stub().returns(testCreatedDate)
      },
      storage,
      catalog: 'cat',
      user: {id: 25}
    });

    // Reject the insertion
    createStub.returns(Task.rejected('Error'));

    const testFolder = folder.new(null, 'Folder One');

    const beforeHandler = sinon.stub();
    repo.events.on(`before:create:${folder.entityType}`, beforeHandler);
    const afterHandler = sinon.stub();
    repo.events.on(`after:create:${folder.entityType}`, afterHandler);

    const result = task2Promise(repo.create(testFolder));

    return when.all([
      expect(beforeHandler).to.be.called,
      expect(afterHandler).to.not.be.called,
      expect(result).to.be.rejected
    ]);
  });

  describe('should emit before:update event for entities', function(){

    const transactStub = sinon.stub();
    const createStub = sinon.stub();
    const updateStub = sinon.stub();

    const trxStorage = {
      entities: {
        create: createStub,
        update: updateStub
      }
    };

    transactStub.callsFake(fn =>
      fn(
        trxStorage,
        () => {throw Error('Commit should not be explicitly called');},
        () => {throw Error('Rollback should not be explicitly called');}));

    const storage = {
      transact: transactStub
    };

    const repo = entityRepositoryFac({
      timeStamp: {
        now: sinon.stub().returns(testCreatedDate)
      },
      storage,
      catalog: 'cat',
      user: {id: 25}
    });

    updateStub.callsFake((cat, entities) => Task.of(entities));
    createStub.callsFake((cat, entities) => Task.of(entities));

    function testEntity(e){
      const handler = sinon.stub();
      repo.events.on(`before:update:${e.entityType}`, handler);
      const result = task2Promise(repo.update(e));

      const entityPassedToHandler = handler.args[0][0];
      expect(entityPassedToHandler).to.contain(e);
      return result;
    }

    it(' -- Folders', function(){
      return testEntity(folder.new(1, 'Test Folder'));
    });
    it(' -- DataSets', function(){
      return testEntity(dataSet.new(2, 'Test DataSet'));
    });
    it(' -- Variables', function(){
      return testEntity(variable.newCategorical(3, 'Test Variable'));
    });
    it(' -- Attributes', function(){
      return testEntity(attribute.new(4, 'Test Attribute', 4));
    });
    it(' -- Users', function(){
      return testEntity(user.new(5, 'Test User'));
    });
    it(' -- Actions', function(){
      return testEntity(action.new(7, 'Test Action', 5, 'subject', 4, 'action'));
    });

    it('including multiple entities', function(){
      const testFolders = [
        folder.new(1, 'Folder One'),
        folder.new(2, 'Folder Two')
      ];

      const handler = sinon.stub();
      repo.events.on(`before:update:${folder.entityType}`, handler);
      const result = task2Promise(repo.update(testFolders));

      const handlerArgs = handler.args;
      // First call
      expect(handlerArgs[0][0]).to.contain(testFolders[0]);
      // Second call
      expect(handlerArgs[1][0]).to.contain(testFolders[1]);
      return result;
    });

  });

  describe('should emit after:update event for entities', function(){

    const transactStub = sinon.stub();
    const createStub = sinon.stub();
    const updateStub = sinon.stub();

    const trxStorage = {
      entities: {
        create: createStub,
        update: updateStub
      }
    };

    transactStub.callsFake(fn =>
      fn(
        trxStorage,
        () => {throw Error('Commit should not be explicitly called');},
        () => {throw Error('Rollback should not be explicitly called');}));

    const storage = {
      transact: transactStub
    };

    const repo = entityRepositoryFac({
      timeStamp: {
        now: sinon.stub().returns(testCreatedDate)
      },
      storage,
      catalog: 'cat',
      user: {id: 25}
    });

    updateStub.callsFake((cat, entities) => Task.of(entities));
    createStub.callsFake((cat, entities) => Task.of(entities));

    function testEntity(e){
      const handler = sinon.stub();
      repo.events.on(`after:update:${e.entityType}`, handler);
      const result = task2Promise(repo.update(e));

      const entityPassedToHandler = handler.args[0][0];
      expect(entityPassedToHandler).to.contain(e);
      return result;
    }

    it(' -- Folders', function(){
      return testEntity(folder.new(1, 'Test Folder'));
    });
    it(' -- DataSets', function(){
      return testEntity(dataSet.new(2, 'Test DataSet'));
    });
    it(' -- Variables', function(){
      return testEntity(variable.newCategorical(3, 'Test Variable'));
    });
    it(' -- Attributes', function(){
      return testEntity(attribute.new(4, 'Test Attribute', 4));
    });
    it(' -- Users', function(){
      return testEntity(user.new(5, 'Test User'));
    });
    it(' -- Actions', function(){
      return testEntity(action.new(7, 'Test Action', 5, 'subject', 4, 'action'));
    });

    it('including multiple entities', function(){
      const testFolders = [
        folder.new(1, 'Folder One'),
        folder.new(2, 'Folder Two')
      ];

      const handler = sinon.stub();
      repo.events.on(`after:update:${folder.entityType}`, handler);
      const result = task2Promise(repo.update(testFolders));

      const handlerArgs = handler.args;
       //First call
      expect(handlerArgs[0][0]).to.contain(testFolders[0]);
       //Second call
      expect(handlerArgs[1][0]).to.contain(testFolders[1]);

      return result;
    });

  });

  it('should not emit update events (before and after) if the entity validation fails', function(){

    const transactStub = sinon.stub();
    const createStub = sinon.stub();
    const updateStub = sinon.stub();

    const trxStorage = {
      entities: {
        create: createStub,
        update: updateStub
      }
    };

    transactStub.callsFake(fn =>
      fn(
        trxStorage,
        () => {throw Error('Commit should not be explicitly called');},
        () => {throw Error('Rollback should not be explicitly called');}));

    const storage = {
      transact: transactStub
    };

    const repo = entityRepositoryFac({
      timeStamp: {
        now: sinon.stub().returns(testCreatedDate)
      },
      storage,
      catalog: 'cat',
      user: {id: 25}
    });

    updateStub.callsFake((cat, entities) => Task.of(entities));
    createStub.callsFake((cat, entities) => Task.of(entities));

    // Giving this no ID will cause validation to fail
    const testFolder = folder.new(null, 'Folder One');

    const beforeHandler = sinon.stub();
    repo.events.on(`before:update:${folder.entityType}`, beforeHandler);
    const afterHandler = sinon.stub();
    repo.events.on(`after:update:${folder.entityType}`, afterHandler);

    const result = task2Promise(repo.update(testFolder));

    return when.all([
      expect(beforeHandler).to.not.be.called,
      expect(afterHandler).to.not.be.called,
      expect(result).to.be.rejected
    ]);
  });

  it('should not emit update events (after) if storage throws an error', function(){

    const transactStub = sinon.stub();
    const createStub = sinon.stub();
    const updateStub = sinon.stub();

    const trxStorage = {
      entities: {
        create: createStub,
        update: updateStub
      }
    };

    transactStub.callsFake(fn =>
      fn(
        trxStorage,
        () => {throw Error('Commit should not be explicitly called');},
        () => {throw Error('Rollback should not be explicitly called');}));

    const storage = {
      transact: transactStub
    };

    const repo = entityRepositoryFac({
      timeStamp: {
        now: sinon.stub().returns(testCreatedDate)
      },
      storage,
      catalog: 'cat',
      user: {id: 25}
    });

    // Reject the update
    updateStub.returns(Task.rejected('Error'));
    createStub.callsFake((cat, entities) => Task.of(entities));

    const testFolder = folder.new(2, 'Folder One');

    const beforeHandler = sinon.stub();
    repo.events.on(`before:update:${folder.entityType}`, beforeHandler);
    const afterHandler = sinon.stub();
    repo.events.on(`after:update:${folder.entityType}`, afterHandler);

    const result = task2Promise(repo.update(testFolder));

    return when.all([
      expect(beforeHandler).to.be.called,
      expect(afterHandler).to.not.be.called,
      expect(result).to.be.rejected
    ]);
  });

  describe('should emit before:delete event for entities', function(){

    const storage = {
      transact: sinon.stub(),
      entities: {
        query: sinon.stub(),
        delete: sinon.stub(),
        create: sinon.stub()
      }
    };

    const dataDelete = sinon.stub();

    const repo = entityRepositoryFac({
      timeStamp: {
        now: sinon.stub().returns(testCreatedDate)
      },
      dataDelete,
      storage,
      catalog: 'cat',
      user: {id: 25}
    });

    storage.entities.delete.callsFake((cat, type, ids) => Task.of(ids));
    storage.entities.query.returns(Task.of([]));
    storage.entities.create.callsFake((cat, entities) => Task.of(entities));

    dataDelete.returns(Task.of('Success'));

    storage.transact.callsFake(fn =>
      fn(
        R.dissoc('transact', storage),
        () => {throw Error('Commit should not be explicitly called');},
        () => {throw Error('Rollback should not be explicitly called');}));

    function testEntity(entityType, entityId){
      const handler = sinon.stub();
      repo.events.on(`before:delete:${entityType}`, handler);
      const result = task2Promise(repo.delete(entityType, entityId));

      const idPassedToHandler = handler.args[0][0];
      expect(idPassedToHandler).to.equal(entityId);
      return result;
    }

    it(' -- Folders', function(){
      return testEntity(folder.entityType, 1);
    });
    it(' -- DataSets', function(){
      return testEntity(dataSet.entityType, 1);
    });
    it(' -- Variables', function(){
      return testEntity(variable.entityType, 1);
    });
    it(' -- Attributes', function(){
      return testEntity(attribute.entityType, 1);
    });
    it(' -- Users', function(){
      return testEntity(user.entityType, 1);
    });
    it(' -- Actions', function(){
      return testEntity(action.entityType, 1);
    });

    it('including multiple entities', function(){
      const handler = sinon.stub();
      repo.events.on(`before:delete:${folder.entityType}`, handler);
      const result = task2Promise(repo.delete(folder.entityType, [1,2]));

      const handlerArgs = handler.args;
      // First call
      expect(handlerArgs[0][0]).to.equal(1);
      // Second call
      expect(handlerArgs[1][0]).to.equal(2);
      return result;
    });
  });

  describe('should emit after:delete event for entities', function(){

    const storage = {
      transact: sinon.stub(),
      entities: {
        query: sinon.stub(),
        delete: sinon.stub(),
        create: sinon.stub()
      }
    };

    const dataDelete = sinon.stub();

    const repo = entityRepositoryFac({
      timeStamp: {
        now: sinon.stub().returns(testCreatedDate)
      },
      dataDelete,
      storage,
      catalog: 'cat',
      user: {id: 25}
    });

    storage.entities.delete.callsFake((cat, type, ids) => Task.of(ids));
    storage.entities.query.returns(Task.of([]));
    storage.entities.create.callsFake((cat, entities) => Task.of(entities));
    dataDelete.returns(Task.of('Success'));

    storage.transact.callsFake(fn =>
      fn(
        R.dissoc('transact', storage),
        () => {throw Error('Commit should not be explicitly called');},
        () => {throw Error('Rollback should not be explicitly called');}));

    storage.entities.delete.callsFake((cat, type, ids) => Task.of(ids));

    function testEntity(entityType, entityId){
      const handler = sinon.stub();
      repo.events.on(`after:delete:${entityType}`, handler);
      const result = task2Promise(repo.delete(entityType, entityId));

      const idPassedToHandler = handler.args[0][0];
      expect(idPassedToHandler).to.equal(entityId);
      return result;
    }

    it(' -- Folders', function(){
      testEntity(folder.entityType, 1);
    });
    it(' -- DataSets', function(){
      testEntity(dataSet.entityType, 1);
    });
    it(' -- Variables', function(){
      testEntity(variable.entityType, 1);
    });
    it(' -- Attributes', function(){
      testEntity(attribute.entityType, 1);
    });
    it(' -- Users', function(){
      testEntity(user.entityType, 1);
    });
    it(' -- Actions', function(){
      testEntity(action.entityType, 1);
    });

    it('including multiple entities', function(){
      const handler = sinon.stub();
      repo.events.on(`after:delete:${folder.entityType}`, handler);
      const result = task2Promise(repo.delete(folder.entityType, [1,2]));

      const handlerArgs = handler.args;
       //First call
      expect(handlerArgs[0][0]).to.equal(1);
       //Second call
      expect(handlerArgs[1][0]).to.equal(2);
      return result;
    });
  });

  it('should not emit delete events (before and after) if the delete validation fails', function(){

    const storage = {entities: { delete: sinon.stub()}};
    const repo = entityRepositoryFac({
      timeStamp: {
        now: sinon.stub().returns(testCreatedDate)
      },
      storage,
      catalog: 'cat',
      user: {id: 25}
    });

    storage.entities.delete.callsFake((cat, type, ids) => Task.of(ids));

    // Pass bad entity type
    const type = 'badtype';

    const beforeHandler = sinon.stub();
    repo.events.on(`before:delete:${type}`, beforeHandler);
    const afterHandler = sinon.stub();
    repo.events.on(`after:delete:${type}`, afterHandler);

    const result = task2Promise(repo.delete(type, 1));

    return when.all([
      expect(beforeHandler).to.not.be.called,
      expect(afterHandler).to.not.be.called,
      expect(result).to.be.rejected
    ]);
  });

  it('should not emit delete events (after) if storage throws an error', function(){

    const storage = {entities: { delete: sinon.stub()}};
    const repo = entityRepositoryFac({
      timeStamp: {
        now: sinon.stub().returns(testCreatedDate)
      },
      storage,
      catalog: 'cat',
      user: {id: 25}
    });

    // Reject the insertion
    storage.entities.delete.returns(Task.rejected('Error'));

    const beforeHandler = sinon.stub();
    repo.events.on(`before:delete:${folder.entityType}`, beforeHandler);
    const afterHandler = sinon.stub();
    repo.events.on(`after:delete:${folder.entityType}`, afterHandler);

    const result = task2Promise(repo.delete(folder.entityType, 4));

    return when.all([
      expect(beforeHandler).to.be.called,
      expect(afterHandler).to.not.be.called,
      expect(result).to.be.rejected
    ]);
  });

});
