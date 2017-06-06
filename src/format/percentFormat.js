const { isNil } = require('ramda');

module.exports = function(value, options){

  if(isNil(value) || isNaN(value)){
    return '';
  }

  // Multiply by 100 and hack toFixed to force to correct precision
  var processedVal = +(value * 100).toFixed(6);

  if(options && options.round){
    processedVal = +processedVal.toFixed(options.round);
  }

  return processedVal + '%';
};
