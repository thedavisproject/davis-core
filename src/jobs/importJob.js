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

    queue: (dataSet, schema, filePath, queue) => thread(
      queue.add(jobType, {
        dataSet,
        schema,
        filePath
      }),
      Async.fromPromise),

    // Expected job data structure
    // {
    //   dataSet: Int!,
    //   filePath: String!
    // }
    processor: function(job){
      const { dataSet, schema, filePath } = job.data;
      return task2Promise(dataImport(dataSet, schema, filePath));
    }
  };
};
