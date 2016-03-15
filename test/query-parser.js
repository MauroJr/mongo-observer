const debug = require('debug')('test:query-parser'),
      QueryParser = require('../lib/query-parser');

const query = QueryParser({name: 'john doe'});

console.log(query.match({name: 'john de'}));