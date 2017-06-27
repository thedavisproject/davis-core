const R = require('ramda');
const util = require('davis-shared');
const {thread} = util.fp;
const {toArray} = util.array;

module.exports =
  ({
    storage,
    catalog
  }) =>
  {
    // buildResults :: [a] -> Task b
    const buildResults = R.curry(function(requestedDataSetIds, individuals){

      // Group by data set and add the data set id to the results
      return thread(individuals,
        R.groupBy(R.prop('dataSet')),
        R.toPairs,
        R.map(([key, value]) => ({
          dataSet: +key,
          data: value
        })));
    });

    return (filters, dataSetIds) => thread(
      storage.data.query(catalog, filters, dataSetIds),
      R.map(buildResults(toArray(dataSetIds))));
  };
