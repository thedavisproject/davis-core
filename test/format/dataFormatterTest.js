const { expect } = require('chai');
const { variable } = require('davis-model');
const dataFormatter = require('../../src/format/dataFormatter');

describe('Format data', function(){

  const { format } = dataFormatter({
    percent: function(value, optionsIgnored){
      return +(value * 100).toFixed(6) + '%';
    },
    percentWithOptions: function(value, options){
      return options && options.round ?
        Math.round(value * 100) + '%' :
          +(value * 100).toFixed(6) + '%';
    }
  });

  it('should return value as string if no formatters exist', function(){
    const v = variable.newQuantitative('123', 'foo');
    expect(dataFormatter({}).format(v, 54.5)).to.equal('54.5');
  });

  it('should return empty if value is null', function(){
    const v = variable.newQuantitative('123', 'foo');
    expect(dataFormatter({}).format(v, null)).to.equal('');
  });

  it('should return value as string if variable has no formatter', function(){
    const v = variable.newQuantitative('123', 'foo');
    expect(format(v, 54.5)).to.equal('54.5');
  });

  it('should throw if variable has formatter, but no type prop', function(){
    const v = variable.newQuantitative('123', 'foo', {
      format: {
        foo: 'bar'
      }
    });
    expect(() => format(v, 54.5)).to.throw(/Invalid/);
  });

  it('should throw if variable has formatter type that doesnt exist', function(){
    const v = variable.newQuantitative('123', 'foo', {
      format: {
        type: 'bar'
      }
    });
    expect(() => format(v, 54.5)).to.throw(/Invalid/);
  });

  it('should format for valid type', function(){
    const v = variable.newQuantitative('123', 'foo', {
      format: {
        type: 'percent'
      }
    });
    expect(format(v, .545)).to.equal('54.5%');
  });

  it('should format for valid type with options', function(){
    const v = variable.newQuantitative('123', 'foo', {
      format: {
        type: 'percentWithOptions',
        options: {
          round: true
        }
      }
    });
    expect(format(v, .545)).to.equal('55%');
  });
});
