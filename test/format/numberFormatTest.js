const { expect } = require('chai');
const numberFormat = require('../../src/format/numberFormat');

describe('Format number', function(){

  it('should just convert to string if no options', function(){
    expect(numberFormat(456.7)).to.equal('456.7');
  });

  it('should round', function(){
    expect(numberFormat(456.756, {round: 1})).to.equal('456.8');
  });

  it('should round zeros', function(){
    expect(numberFormat(456.00000, {round: 0})).to.equal('456');
  });

  it('should add commas', function(){
    expect(numberFormat(5672456.756, {pretty: true})).to.equal('5,672,456.756');
  });

  it('should return empty if value is null', function(){
    expect(numberFormat(null, {round: 1, pretty: true})).to.equal('');
  });

  it('should return empty if value is NaN', function(){
    expect(numberFormat('Foo', {round: 1, pretty: true})).to.equal('');
  });

});
