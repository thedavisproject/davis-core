const { thread } = require('davis-shared').fp;
const Task = require('data.task');
const Async = require('control.async')(Task);
const when = require('when');
const task2Promise = Async.toPromise(when.promise);

const jobType = 'PUBLISH';

module.exports = ({
  publish
}) => {

  return {
    jobType,

    queue: (target, queue) => thread(
      queue.add(jobType, {
        target
      }),
      Async.fromPromise),

    // Expected job data structure
    // {
    //   target: String!
    // }
    processor: function(job){
      const { target } = job.data;
      return task2Promise(publish(target));
    }
  };
};
