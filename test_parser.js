var util = require('util');
var parser = require('./parse_file.js');

parser.parseFile(process.argv[2], function(d) {
	util.puts("================");
	util.puts(JSON.stringify(d));
});
