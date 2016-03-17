'use strict';

const chai = require('chai'),
      expect = chai.expect,
      QueryParser = require('../lib/query-parser');

describe('Query Parser', function () {
    
    describe('query without operators', function () {
        let query = QueryParser({name: 'John Doe'});
    
        it('should return true on doc has equal field', function () {
            let result = query.match({name: 'John Doe'});
    
            expect(result).to.be.true;
        });
        
        it('should return false on doc has no equal field', function () {
            let result = query.match({name: 'john doe'});
    
            expect(result).to.be.false;
        });
    });
    
    describe('query without operators with implicit "$and"', function () {
        let query = QueryParser({name: 'John Doe', age: 20, id: 321});
        
        it('should return true on all fields is equal', function () {
           let result = query.match({name: 'John Doe', age: 20, id: 321});
           
           expect(result).to.be.true;
        });
        
        it('should return false on least one field is not equal', function () {
           let result = query.match({name: 'John Doe', age: 19, id: 321});
           
           expect(result).to.be.false;
        });
    });
    
    describe('query with "$eq" operator', function () {
        let query = QueryParser({age: {$eq: 18}});
        
        it('should return true on doc has equal field', function () {
            let result = query.match({name: 'John Doe', age: 18});
    
            expect(result).to.be.true;
        });
        
        it('should return false on doc has no equal field', function () {
            let result = query.match({name: 'John Doe', age: 22});
    
            expect(result).to.be.false;
        });
    });
    
});