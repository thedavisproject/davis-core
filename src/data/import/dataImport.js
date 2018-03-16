const R = require('ramda');
const Task = require('data.task');
const when = require('when');
const {dataSet, variable, query: q} = require('davis-model');
const _ = require('highland');
const Async = require('control.async')(Task);
const task2Promise = Async.toPromise(when.promise);
const { thread } = require('davis-shared').fp;

const DEFAULT_BATCH_SIZE = 500;

module.exports =
  ({
    storage,
    catalog,
    timeStamp,
    individualGenerator: { rawToIndividuals },
    parseDataFile
  }) =>
  {
    return (dataSetId, columnMapping, filePath, {
      batchSize = DEFAULT_BATCH_SIZE,
      createMissingAttributes = false
    } = {}) => {

      // Wrap everything in a transaction
      return storage.transact((trx, commit, rollback) => {

        let transactionEnded = false;

        // On errors, roll back the transaction and reject the task
        const handleError = err => {
          if(!transactionEnded){
            transactionEnded = true;
            rollback(err);
          }
        };

        const batchPromises = [];

        thread(
          // First delete the data for this data set
          trx.data.delete(catalog, {dataSet: dataSetId}),
          // Create an individual processor for this data set
          R.chain(() => rawToIndividuals(dataSetId, columnMapping, {
            createMissingAttributes
          })),
          R.map(toIndividuals =>
            // Parse the file to raw individual objects and convert to real individuals
            parseDataFile(filePath).pipe(toIndividuals)))
            // Fork the process to run it immediately
            // Tasks are lazy, so it must be immediately forked to keep the
            // benefits of streaming data directly into the database and not overloading
            // memory.
            .fork(handleError, individualStream => {

              // Handle recording the schma
              let schemaMap = {};

              function recordIndividualsForSchema(individuals){
                individuals.forEach(individual =>
                  individual.facts.forEach(f => {
                    // If the variable has not been recorded yet
                    if(!schemaMap[f.variable]){
                      schemaMap[f.variable] = {
                        variable: f.variable
                      };
                    }

                    if(f.type === variable.types.categorical){
                      if(!schemaMap[f.variable].attributes){
                        schemaMap[f.variable].attributes = {};
                      }
                      if(!schemaMap[f.variable].attributes[f.attribute]){
                        schemaMap[f.variable].attributes[f.attribute] = f.attribute;
                      }
                    }
                  }));
              }

              // Process the data stream with highland.js
              _(individualStream)
                // Batch the individuals for insertion
                .batch(batchSize)
                // If the stream emits an error, handle it
                .stopOnError(handleError)
                // For each batch, convert it to a promise that represents the record insertion
                .each((individualBatch) => {

                  // Record the individuals as they pass by to finaly generate the schema
                  recordIndividualsForSchema(individualBatch);

                  // Wrapping the task in a promise so that it can be immediately
                  // run. Same reason as above.
                  batchPromises.push(task2Promise(trx.data.create(catalog, individualBatch))
                    .catch(handleError));
                })
                .done(() => {
                  when.all(batchPromises)
                    .then(batchCounts => {
                      if(!transactionEnded){

                        const datasetQueryTask = trx.entities.query(
                          catalog,
                          dataSet.entityType,
                          q.build.equals('id', dataSetId));

                        // Simplify Schema
                        const schema = thread(
                          schemaMap,
                          R.values,
                          R.map(R.evolve({attributes: R.values})));

                        datasetQueryTask
                          .chain(ds => {
                            const modifiedDataSet = thread(
                              ds[0],
                              // Set the data set's schema
                              R.assoc('schema', schema),
                              // Set the data set's data modified date
                              dataSet.setDataModified(timeStamp.now()));

                            return trx.entities.update(catalog, [modifiedDataSet]);
                          })
                          .fork(handleError, () => {
                            transactionEnded = true;
                            commit(R.sum(batchCounts));
                          });
                      }
                    }).catch(handleError);
                });
            });
      });
    };
  };
