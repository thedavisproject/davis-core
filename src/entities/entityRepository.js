const R = require('ramda');
const Maybe = require('data.maybe');
const Either = require('data.either');
const Task = require('data.task');
const shared = require('davis-shared');
const {toArray} = shared.array;
const {thread, either2Task} = shared.fp;
const model = require('davis-model');
const {entity, folder, dataSet, variable, attribute, action} = model;
const validEntityTypes = model.entityTypes;
const q = model.query.build;
const EventEmitter = require('events');

const validateEntityType = entity => {
  const type = (typeof entity) === 'string' ? entity : entity.entityType;
  return validEntityTypes.has(type) ?
    Either.Right(entity) :
    Either.Left(`Invalid entity type: ${type}`);
};

const validateEmptyIds = entities => {
  if(R.any(e => !R.isNil(e.id), entities)){
    return Either.Left('Entity objects must have empty id properties when inserting new records.');
  }
  return Either.Right(entities);
};

const validateNoEmptyIds = entities => {
  if(R.any(e => R.isNil(e.id), entities)){
    return Either.Left('Entity objects must not have empty id properties when updating records.');
  }
  return Either.Right(entities);
};

const validateIsHierarchical = e => {
  if(!e.hierarchical){
    return Either.Left(`${e.entityType} is not a hierarchical item`);
  }
  return Either.Right(e);
};

const createRepository =
  ({
    dataDelete,
    timeStamp,
    storage,
    catalog,
    user
  }) =>
  {
    const touchCreatedDate = entity.setCreated(timeStamp.now());
    const touchModifiedDate = entity.setModified(timeStamp.now());

    const events = new EventEmitter();

    const queryAll = entityType => thread(
      validateEntityType(entityType),
      either2Task,
      R.chain(validatedType => storage.entities.query(catalog, validatedType)));

    const queryById = (entityType, ids) => thread(
      validateEntityType(entityType),
      either2Task,
      R.chain(validatedType => storage.entities.query(
        catalog,
        validatedType,
        ['in', 'id', toArray(ids)])));

    const query = (entityType, query, options = {}) => thread(
      validateEntityType(entityType),
      either2Task,
      R.chain(validatedType => storage.entities.query(catalog, validatedType, query, options)));

    const create = entities => thread(
      toArray(entities),
      validateEmptyIds,
      R.chain(R.traverse(Either.of, validateEntityType)),
      R.map(R.map(touchCreatedDate)),
      R.map(R.map(touchModifiedDate)),
      either2Task,
      // Emit a "before" event for each entity that is created
      R.map(R.map(R.tap(e => events.emit(`before:create:${e.entityType}`, e)))),
      R.chain(validatedEntities =>
        // Do the next steps in a transaction
        storage.transact(trx => thread(
          // Create the entities
          trx.entities.create(catalog, validatedEntities),
          // Update the action log
          R.chain(entities => {
            const actionLogEntries = entities.map(e => thread(
              action.entityCreatedEntry(user.id, e),
              touchCreatedDate,
              touchModifiedDate));

            return trx.entities.create(catalog, actionLogEntries)
              .map(() => entities); // Return the original entities, not the action log entries
          })))),
      // Emit a "after" event for each entity that is created
      R.map(R.map(R.tap(e => events.emit(`after:create:${e.entityType}`, e)))));

    const update = entities => thread(
      toArray(entities),
      validateNoEmptyIds,
      R.chain(entities => R.traverse(Either.of, validateEntityType, entities)),
      R.map(R.map(touchModifiedDate)),
      either2Task,
      // Emit a "before" event for each entity that is updated
      R.map(R.map(R.tap(e => events.emit(`before:update:${e.entityType}`, e)))),
      R.chain(validatedEntities =>
        // Do the next steps in a transaction
        storage.transact(trx => thread(
          // Update the entities
          trx.entities.update(catalog, validatedEntities),
          // Update the action log
          R.chain(entities => {
            const actionLogEntries = entities.map(e => thread(
              action.entityUpdatedEntry(user.id, e),
              touchCreatedDate,
              touchModifiedDate));

            return trx.entities.create(catalog, actionLogEntries)
              .map(() => entities); // Return the original entities, not the action log entries
          })))),
      // Emit a "after" event for each entity that is updated
      R.map(R.map(R.tap(e => events.emit(`after:update:${e.entityType}`, e)))));

    const del = (entityType, ids, trxStorage) => thread(
      validateEntityType(entityType),
      either2Task,
      R.chain(() => {
        // If no good entity ids passed in, short circuit the output
        if(!ids || R.isEmpty(ids)){
          return Task.of('No IDs supplied');
        }
        // Normal delete process
        else{
          const beforeEvents = () => toArray(ids).forEach(entityId => {
            events.emit(`before:delete:${entityType}`, entityId);
          });

          const afterEvents = () => toArray(ids).forEach(entityId => {
            events.emit(`after:delete:${entityType}`, entityId);
          });

          const delPipe = trx => thread(
            trx.entities.delete(catalog, entityType, toArray(ids)),
            R.chain(() => {
              const actionLogEntries = toArray(ids).map(id => thread(
                action.entityDeletedEntry(user.id, entityType, id),
                touchCreatedDate,
                touchModifiedDate));

              return trx.entities.create(catalog, actionLogEntries)
                .map(R.T); // Return the original entities, not the action log entries
            }),
            R.chain(() => deleteDependents(trx, entityType, toArray(ids))), // Trigger "after" events after the delete (including dependents, in case the trx fails)
            R.map(R.tap(afterEvents)));

          // Trigger before delete events
          beforeEvents();

          // If a transaction was passed in (this delete is part of a large trx), use that
          if(trxStorage){
            return delPipe(trxStorage);
          }
          // Otherwise kick off a new trx
          else{
            return storage.transact(delPipe);
          }
        }
      }),
      // Change the internal result to "True" (when the Task completes successfully)
      R.map(R.T));

    // TODO: This should be moved somewhere else and made configurable
    const deleteDependents = (trxStorage, entityType, ids) => {

      const findAndDelete = (eType, queryExp) => thread(
        query(eType, queryExp),
        R.map(R.map(e => e.id)),
        R.chain(foundIds => del(eType, foundIds, trxStorage)));

      let deleteTasks = [];

      if(entityType === folder.entityType){
        deleteTasks = [
          // Dependent folders (children)
          findAndDelete(folder.entityType, q.in('parent', ids)),
          // Dependent dataSets
          findAndDelete(dataSet.entityType, q.in('folder', ids))
        ];
      }
      else if(entityType === dataSet.entityType){
        deleteTasks = [
          // Dependent variables
          findAndDelete(variable.entityType, q.in('scopedDataSet', ids)),
          // Data
          dataDelete({dataSet: ids})
        ];
      }
      else if(entityType === variable.entityType){
        deleteTasks = [
          // Dependent attributes
          findAndDelete(attribute.entityType, q.in('variable', ids)),
          // Data
          dataDelete({variable: ids})
        ];
      }
      else if(entityType === attribute.entityType){
        deleteTasks = [
          // Dependent attributes (children)
          findAndDelete(attribute.entityType, q.in('parent', ids)),
          // Data
          dataDelete({attribute: ids})
        ];
      }

      return deleteTasks.length > 0 ?
        R.sequence(Task.of, deleteTasks) :
        Task.of('No dependents');
    };

    const getParent = R.pipe(
      validateIsHierarchical,
      either2Task,
      R.chain(entity =>
        model.entity.isValidId(entity.parent) ?
        storage.entities.query(catalog, entity.entityType, q.equals('id', entity.parent)) :
        Task.of([])),
      R.map(R.pipe(
        R.head,
        Maybe.fromNullable)));

    const getChildren = R.pipe(
      validateIsHierarchical,
      either2Task,
      R.chain(entity =>
        storage.entities.query(catalog, entity.entityType, q.equals('parent', entity.id))));

    return {
      queryAll,
      queryById,
      query,
      create,
      update,
      delete: del,
      getParent,
      getChildren,
      events
    };
  };

module.exports = ({
  dataDelete,
  timeStamp,
  storage,
  catalog,
  user
}) => {

  const dependencies = {
    dataDelete,
    timeStamp,
    storage,
    catalog,
    user
  };

  const repo = createRepository(dependencies);

  repo.transact = trx => createRepository(Object.assign({},
    dependencies,
    {
      storage: trx
    }));

  return repo;
};
