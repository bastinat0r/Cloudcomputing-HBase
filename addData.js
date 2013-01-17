var util = require('util');
var parser = require('./parse_file.js');
var hbase = require('hbase');


var tablename = 'location';

// create table if !exists
var table = hbase({ host: '127.0.0.1', port: 8090 }).getTable(tablename);

function handleData(data) {
	data.fieldNames.shift();
	data.fieldNames.shift();
	table.create('cf', function(err, succ) {
		if(err)
			util.puts(err);
		for(var i in data.fields) {
			createOrUpdateRow(data.fields[i].shift()+"_"+data.fields[i].shift(), data.fieldNames, data.fields[i]);
		};
//	createOrUpdateRow('test_row', ['test_column1','test_column2'],['test_value1', 'test_value2']);
	});
}

function createOrUpdateRow(rowKey, columns, data) {
	for(var i in data) {
		var columnstr = "cf:" + columns[i].split(":")[0];
		updateCell(rowKey, columnstr, data[i]);
	}
}

function updateCell(rowKey, column, data) {
	var row = table.getRow(rowKey).put(column, data, function(err, succ) {
		if(err)
			util.puts(err);
	});
};

parser.parseFile(process.argv[2], handleData);
