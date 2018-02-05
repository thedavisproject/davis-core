const { thread } = require('davis-shared').fp;
const Task = require('data.task');
const Async = require('control.async')(Task);
const when = require('when');
const task2Promise = Async.toPromise(when.promise);

const jobType = 'IMPORT';

module.exports = ({
  dataImport
}) => {

  return {
    jobType,

    queue: ({dataSet, columnMappings, filePath, createMissingAttributes = false}, queue) => thread(
      queue.add(jobType, {
        dataSet,
        columnMappings,
        filePath,
        createMissingAttributes
      }),
      Async.fromPromise),

    // Expected job data structure
    // {
    //   dataSet: Int!,
    //   columnMappings: Object!,
    //   filePath: String!
    //   createMissingAttributes: Bool!
    // }
    processor: function(job){
      const { dataSet, columnMappings, filePath, createMissingAttributes = false } = job.data;
      return task2Promise(dataImport(dataSet, columnMappings, filePath, {
        createMissingAttributes
      }));
    }
  };
};
