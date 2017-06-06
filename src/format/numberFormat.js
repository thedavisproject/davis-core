const { isNil } = require('ramda');

module.exports = function(value, options){

  if(isNil(value) || isNaN(value)){
    return '';
  }

  var processedVal = value;

  if(options && options.round){
    processedVal = +processedVal.toFixed(options.round);
  }

  if(options && options.pretty){
    return processedVal.toLocaleString();
  }

  return processedVal.toString();
};
