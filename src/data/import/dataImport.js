const R = require('ramda');
const Task = require('data.task');
const when = require('when');
const {dataSet, query: q} = require('davis-model');
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
    return (dataSetId, filePath, batchSize = DEFAULT_BATCH_SIZE) =>
      // Wrap everything in a transaction
      storage.transact((trx, commit, rollback) => {

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
          // Create an individual processor for this data set
          rawToIndividuals(dataSetId),
          R.map(toIndividuals =>
            // Parse the file to raw individual objects and convert to real individuals
            parseDataFile(filePath).pipe(toIndividuals)))
            // Fork the process to run it immediately 
            // Tasks are lazy, so it must be immediately forked to keep the
            // benefits of streaming data directly into the database and not overloading
            // memory.
            .fork(handleError, individualStream => {
              // Process the data stream with highland.js
              _(individualStream)
                // Batch the individuals for insertion
                .batch(batchSize)
                // If the stream emits an error, handle it
                .stopOnError(handleError)
                // For each batch, convert it to a promise that represents the record insertion
                .each((individualBatch) => {
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

                        datasetQueryTask
                          .chain(ds => {
                            const modifiedDataSet = dataSet.setDataModified(timeStamp.now(), ds[0]);
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
