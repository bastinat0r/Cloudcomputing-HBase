var util = require('util');
var hbase = require('hbase');

var commentNumber = 0;
var tablename = 'location';

// create table if !exists
var hb = hbase({ host: '127.0.0.1', port: 8090 });
var table = hb.getTable(tablename); 

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

var rowKeys = [];
function randomRowKey(cb) {
	if(rowKeys.length < 1) {	
		table.getRow("*").get('cf:Timestamp', function(err, val) {
			if(err)
				util.puts(err);
			rowKeys = val.map(function(d) {return d.key});
			cb(rowKeys[Math.floor(Math.random() * rowKeys.length)]);
		});
	} else {
		cb(rowKeys[Math.floor(Math.random() * rowKeys.length)]);
	}
}

function printCell(rowKey, column) {
	table.getRow(rowKey).get(column, function(err, value) {
		if(err) {
			util.puts(err);
		} else {
			if(value != null)
				util.puts(JSON.stringify(value[0]["$"]));
		}
	});
}



function fetchComment(rowKey, cb, iteration, upper) {
	if(iteration == null)
		iteration = 0;

	table.getRow(rowKey).exists("comment:author"+iteration, function(err, exists) {
		if(err)
			util.puts(err);
		if(exists) {
			

			util.puts("rk: " + rowKey);
			printCell(rowKey, "comment:author"+iteration);
			printCell(rowKey, "comment:text"+iteration);
			printCell(rowKey, "comment:num"+iteration);
			
			if(upper == null || iteration < upper) {
				fetchComment(rowKey, cb, iteration+1, upper);
			}

		} else {
			if(cb)
				cb();
		}
	});
};

fetchComment("DE_BERLIN_10589_TIMELESS_2520714258637237151");
