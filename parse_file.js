var fs = require('fs');
var csv = require('csv');
var util = require('util');

function parseFile(fileName, cb) {
	fs.readFile(fileName, function(err, data) {
		if(err) {
			util.puts(JSON.stringify(err));
		}
		if(data != null) {
			csv().from.string("" + data).to.array(function(d) {
				var obj = {
					fieldNames : d.shift(),
					fields : d
				};
				for(var i in obj.fieldNames) {
					util.puts("fieldName: " + obj.fieldNames[i]);
				}
				if(cb)
					cb(obj);
			});
		}
	});
};

module.exports.parseFile = parseFile;
