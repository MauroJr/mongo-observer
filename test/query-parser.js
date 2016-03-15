'use strict';

const chai = require('chai'),
      expect = chai.expect,
      QueryParser = require('../lib/query-parser');

describe('Query Parser', function () {
    
    describe('simple query without operators', function () {
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
    
});