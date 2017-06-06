const R = require('ramda');
const model = require('davis-model');
const shared = require('davis-shared');
const {thread} = shared.fp;
const {action, dataSet} = model;
const validEntityTypes = model.entityTypes;
const q = model.query.build;
const querySort = model.query.sort;

module.exports =
  ({
    entityRepository,
    storage,
    catalog,
    user
  }) =>
  {

    return target => {

      return storage.transact(trx => {

        const entityRepositoryTransacted = entityRepository.transact(trx);

        const lastFullPublish = entityRepositoryTransacted.query(
          action.entityType,
          q.eq('action', action.actions.publish.full),
          {
            sort: querySort.desc('created'),
            take: 1
          });

        return lastFullPublish.chain(lastPublishResults => {

          const lastPublishTime = lastPublishResults.length === 0 ?
            new Date(0) :
            lastPublishResults[0].created;

          const dataSetsPublishedSinceLastTime = entityRepositoryTransacted.query(
            dataSet.entityType,
            q.gt('dataModified', lastPublishTime));

          const publishAllEntities = () => trx.publish.publishEntities(
            catalog,
            target,
            [...validEntityTypes]);

          const publishChangedData = () => dataSetsPublishedSinceLastTime.chain(dataSets =>
            trx.publish.publishFacts(
              catalog,
              target,
              dataSets.map(d => d.id)));

          const addFullSitePublishEntry = () => {
            const logEntry = action.fullPublishEntry(user.id);

            return entityRepositoryTransacted.create(logEntry);
          };

          return thread(
            // Publish entities
            publishAllEntities(),
            // Publish data that has changed since last publish
            R.chain(publishChangedData),
            // Add another full site publish entry
            R.chain(addFullSitePublishEntry),
            // Return true
            R.map(R.T));
        });
      });
    };
  };
