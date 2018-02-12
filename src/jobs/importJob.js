const { thread } = require('davis-shared').fp;
const Task = require('data.task');
const Async = require('control.async')(Task);

const jobType = 'IMPORT';

module.exports = ({
  dataImport
}) => {

  return {
    jobType,

    queue: ({
      dataSet,
      columnMappings,
      filePath,
      createMissingAttributes = false
    }, queue) => thread(
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
    processor: function(job, done){
      const { dataSet, columnMappings, filePath, createMissingAttributes = false } = job.data;

      dataImport(dataSet, columnMappings, filePath, {
        createMissingAttributes
      }).fork(
        error => error instanceof Error ? done(error) : done(new Error(error)),
        success => done(null, success));
    }
  };
};
