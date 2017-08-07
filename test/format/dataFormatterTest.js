const { expect } = require('chai');
const { variable } = require('davis-model');
const dataFormatter = require('../../src/format/dataFormatter');

describe('Format data', function(){

  const { format } = dataFormatter({
    percent: function(value, optionsIgnored){
      return +(value * 100).toFixed(6) + '%';
    },
    fancyText: function(value, optionsIgnored){
      return `__${value}__`;
    },
    percentWithOptions: function(value, options){
      return options && options.round ?
        Math.round(value * 100) + '%' :
          +(value * 100).toFixed(6) + '%';
    }
  });

  it('should return value as string if no formatters exist', function(){
    const v1 = variable.newNumerical('123', 'foo');
    const v2 = variable.newText('123', 'foo');
    expect(dataFormatter({}).format(v1, 54.5)).to.equal('54.5');
    expect(dataFormatter({}).format(v2, 'foo')).to.equal('foo');
  });

  it('should return empty if value is null', function(){
    const v1 = variable.newNumerical('123', 'foo');
    const v2 = variable.newText('123', 'foo');
    expect(dataFormatter({}).format(v1, null)).to.equal('');
    expect(dataFormatter({}).format(v2, null)).to.equal('');
  });

  it('should return value as string if variable has no formatter', function(){
    const v1 = variable.newNumerical('123', 'foo');
    const v2 = variable.newText('123', 'foo');
    expect(format(v1, 54.5)).to.equal('54.5');
    expect(format(v2, 'foo')).to.equal('foo');
  });

  it('should throw if variable has formatter, but no type prop', function(){
    const v = variable.newNumerical('123', 'foo', {
      format: {
        foo: 'bar'
      }
    });
    expect(() => format(v, 54.5)).to.throw(/Invalid/);
  });

  it('should throw if variable has formatter type that doesnt exist', function(){
    const v = variable.newNumerical('123', 'foo', {
      format: {
        type: 'bar'
      }
    });
    expect(() => format(v, 54.5)).to.throw(/Invalid/);
  });

  it('should format for valid type - numerical', function(){
    const v = variable.newNumerical('123', 'foo', {
      format: {
        type: 'percent'
      }
    });
    expect(format(v, .545)).to.equal('54.5%');
  });

  it('should format for valid type - text', function(){
    const v = variable.newText('123', 'foo', {
      format: {
        type: 'fancyText'
      }
    });
    expect(format(v, 'foo')).to.equal('__foo__');
  });

  it('should format for valid type with options', function(){
    const v = variable.newNumerical('123', 'foo', {
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
