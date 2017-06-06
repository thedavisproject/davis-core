const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
const sinonChai = require('sinon-chai');

chai.use(sinonChai);
chai.use(chaiAsPromised);

const {expect} = chai;

const Task = require('data.task');
const Async = require('control.async')(Task);
const when = require('when');
const task2Promise = Async.toPromise(when.promise);

const sinon = require('sinon');

const deleteFac = require('../../src/data/dataDelete');

describe('Data Delete', function(){
  // Set up for query methods
  const stubbIt = () => {
    const deleteStub = sinon.stub();
    const storage = {
      data: {
        delete: deleteStub
      }
    };
    const del = deleteFac({
      storage,
      catalog: 'cat'
    });
    return {deleteStub, storage, del};
  };

  it('should bubble up Task errors from storage.data.delete', function(){
    const {deleteStub, del} = stubbIt();
    deleteStub.returns(Task.rejected('Error message'));
    const result = task2Promise(del({
      dataSet: 5
    }));
    return expect(result).to.be.rejectedWith('Error message');
  });

  it('should error on bad entity id', function(){
    const {del} = stubbIt();
    const result = task2Promise(del({
      dataSet: 'foo'
    }));
    return expect(result).to.be.rejectedWith(/Invalid filter parameters/);
  });

  it('should call delete with catalog and filters', function(){
    const {deleteStub, del} = stubbIt();
    deleteStub.returns(Task.of(true));

    const result = task2Promise(del({
      dataSet: [3, 4],
      variable: [4, 5],
      attribute: [5]
    }));

    return when.all([
      expect(deleteStub).to.have.been.calledWith('cat', {
        dataSet: [3, 4],
        variable: [4, 5],
        attribute: [5]
      }),
      expect(result).to.eventually.be.true
    ]);
  });

  it('should convert single ids to arrays', function(){
    const {deleteStub, del} = stubbIt();
    deleteStub.returns(Task.of(true));

    const result = task2Promise(del({
      dataSet: 3,
      variable: 4,
      attribute: 5
    }));

    return when.all([
      expect(deleteStub).to.have.been.calledWith('cat', {
        dataSet: [3],
        variable: [4],
        attribute: [5]
      }),
      expect(result).to.eventually.be.true
    ]);
  });

  it('should throw away non-dataSet/variable/attribute filters', function(){
    const {deleteStub, del} = stubbIt();
    deleteStub.returns(Task.of(true));

    const result = task2Promise(del({
      dataSet: [3],
      foo: [4]
    }));

    return when.all([
      expect(deleteStub).to.have.been.calledWith('cat', {
        dataSet: [3]
      }),
      expect(result).to.eventually.be.true
    ]);
  });

  it('should throw away filters that are null or empty', function(){
    const {deleteStub, del} = stubbIt();
    deleteStub.returns(Task.of(true));

    const result = task2Promise(del({
      dataSet: [3],
      variable: null,
      attribute: []
    }));

    return when.all([
      expect(deleteStub).to.have.been.calledWith('cat', {
        dataSet: [3]
      }),
      expect(result).to.eventually.be.true
    ]);
  });
});
