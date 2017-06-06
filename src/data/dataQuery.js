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

      const requestedDataSetMap = thread(
        requestedDataSetIds,
        R.indexBy(R.identity),
        R.map(() => []));

      const individualsByDataSet = thread(
        individuals,
        R.groupBy(R.prop('dataSet')),
        R.map(R.map(i => i.facts)));

      // Merge the empty data set map with the individuals by data set.
      // This will create empty individual sets for requested ids that don't
      // have any data.
      return R.merge(requestedDataSetMap, individualsByDataSet);
    });

    return (filters, dataSetIds) => thread(
      storage.data.query(catalog, filters, dataSetIds),
      R.map(buildResults(toArray(dataSetIds))));
  };
