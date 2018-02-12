const { thread } = require('davis-shared').fp;
const Task = require('data.task');
const Async = require('control.async')(Task);

const jobType = 'PUBLISH';

module.exports = ({
  publish
}) => {

  return {
    jobType,

    queue: ({target}, queue) => thread(
      queue.add(jobType, {
        target
      }),
      Async.fromPromise),

    // Expected job data structure
    // {
    //   target: String!
    // }
    processor: function(job, done){
      const { target } = job.data;
      publish(target).fork(
        error => error instanceof Error ? done(error) : done(new Error(error)),
        success => done(null, success));
    }
  };
};
