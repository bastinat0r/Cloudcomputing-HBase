var fs = require('fs');
var util = require('util');

function parseFile(fileName) {
	fs.readFile(fileName, function(err, data) {
		if(err) {
			util.puts(JSON.stringify(err));
		}
		if(data != null) {
			data = (""+data).split(/\n/g);
			var obj = {
				fieldNames : data.shift().split(","),
				fields : []
			}
			for(var i in data) {
				obj.fields.push(data[i].split(","));
			}
			for(var i in obj.fieldNames) {
				util.puts("found attributes: " + obj.fieldNames[i]);
			}
		}
	});
};

module.exports.parseFile = parseFile;
