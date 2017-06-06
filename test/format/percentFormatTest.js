const { expect } = require('chai');
const percentFormat = require('../../src/format/percentFormat');

describe('Format number', function(){

  it('should multiply by 100', function(){
    expect(percentFormat(.256)).to.equal('25.6%');
  });

  it('should round', function(){
    expect(percentFormat(.56756, {round: 1})).to.equal('56.8%');
  });

  it('should return empty if value is null', function(){
    expect(percentFormat(null, {round: 1})).to.equal('');
  });

  it('should return empty if value is NaN', function(){
    expect(percentFormat('foo', {round: 1})).to.equal('');
  });

});
