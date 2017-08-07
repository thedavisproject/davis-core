const R = require('ramda');
const stringify = require('csv-stringify');
const Task = require('data.task');
const Async = require('control.async')(Task);
const { thread } = require('davis-shared').fp;
const { dataSet, variable, attribute } = require('davis-model');

const liftedStringify = Async.liftNode(stringify);
const toCsv = data => liftedStringify(data, {
  delimiter: ','
});

const getValue = (variables, attributes, fact) => {
  const v = variables[fact.variable];

  if(v.type === variable.types.categorical){
    if(R.isNil(fact.attribute)){
      return '';
    }
    return attributes[fact.attribute].key;
  }
  else if(v.type === variable.types.numerical){
    if(isNaN(fact.value)){
      return '';
    }
    return fact.value;
  }
  else if(v.type === variable.types.text){
    if(R.isNil(fact.value)){
      return '';
    }
    return fact.value;
  }
  else{
    throw `Bad variable type, or no variable match: ${fact.variable}`;
  }
};

const toRow = R.curry(function(headers, facts){
  return headers.map(h => facts[h]);
});

const getRow = R.curry((headers, variables, attributes, individual) => thread(
  individual.facts,
  R.map(fact => [variables[fact.variable].key, getValue(variables, attributes, fact)]),
  R.fromPairs,
  toRow(headers)));

const generateCsvForSingleDataSet = (dataSetData, variables, attributes) => {

  var headers = thread(
    dataSetData[0].facts,
    R.map(f => variables[f.variable].key));

  return [headers].concat(dataSetData.map(getRow(headers, variables, attributes)));
};

module.exports =
  ({
    entityRepository,
    dataQuery
  }) =>
  {
    // queryResults -> Task(csv rows)
    const queryResultsToCsvRows = function(queryResults){


      // First we need to get all data set, variable and attribute entities
      const dataSetIds =  queryResults.map(result => +result.dataSet);

      // Ex. {1: [[f1, f2], [f2,f3]], 2: [[f1, f2], [f2,f3]]}
      const flatFacts = thread(
        queryResults,
        R.map(result => result.data),
        R.flatten,
        R.map(i => i.facts),
        R.flatten
      );

      const variableIds = thread(
        flatFacts,
        R.map(f => f.variable),
        R.uniq);

      const attributeIds = thread(
        flatFacts,
        R.filter(f => f.type === variable.types.categorical),
        R.map(f => f.attribute),
        R.uniq);

      const dataSets =
        entityRepository.queryById(dataSet.entityType, dataSetIds)
          .map(R.indexBy(R.prop('id')));

      const variables =
        entityRepository.queryById(variable.entityType, variableIds)
          .map(R.indexBy(R.prop('id')));

      const attributes =
        entityRepository.queryById(attribute.entityType, attributeIds)
          .map(R.indexBy(R.prop('id')));

      const entities = R.sequence(Task.of, [dataSets, variables, attributes]);

      return thread(
        queryResults,
        R.map(result => thread(
            entities,
            R.chain(([dataSets, variables, attributes]) => thread(
              generateCsvForSingleDataSet(result.data, variables, attributes),
              toCsv,
              R.map(csv => ({
                dataSet: dataSets[+result.dataSet],
                csv
              })))))),
        R.sequence(Task.of));
    };

    return {
      export: R.pipe(
        dataQuery,
        R.chain(queryResultsToCsvRows))
    };
  };
