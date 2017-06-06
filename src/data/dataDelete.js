const {entity} = require('davis-model');
const Task = require('data.task');
const R = require('ramda');
const shared = require('davis-shared');
const {thread} = shared.fp;
const {toArray} = shared.array;

module.exports =
  ({
    storage,
    catalog
  }) =>
  {
    // Data filters: { dataSet: [...], variable: [...], attribute: [...] }
    return filters => {

      // Preprocess filters
      // Clean bad/empty filters
      const cleanedFilters = thread(
        filters,
        R.pick(['dataSet', 'variable', 'attribute']),
        R.reject(R.isNil),
        R.reject(R.isEmpty),
        R.map(toArray));

      const hasBadId = R.any(R.complement(entity.isValidId));

      // If any ids passed in are bad, reject
      if(R.any(hasBadId, R.values(cleanedFilters))){
        return Task.rejected(`Invalid filter parameters. Bad id: ${filters}`);
      }

      return storage.data.delete(catalog, cleanedFilters);
    };
  };
