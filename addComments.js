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

table.create('comment', function(err, succ) {
	if(err)
		util.puts(err);
});

function createComment(rowKey, cb, iteration) {
	if(iteration == null)
		iteration = 0;

	table.getRow(rowKey).exists("comment:author"+iteration, function(err, exists) {
		if(err)
			util.puts(err);
		if(exists) {
			createComment(rowKey, cb, iteration+1);
		} else {
			updateCell(rowKey, 'comment:author'+iteration, ['foo','bar', 'baz', 'blarg'][Math.floor(Math.random() * 4)]);
			updateCell(rowKey, 'comment:text'+iteration, "text..." + Math.random()*3.1415);
			updateCell(rowKey, 'comment:num'+iteration, commentNumber);
			commentNumber++;
			if(cb)
				cb();
		}
	});
};

function troll() {
	randomRowKey(function(rk) {
		util.puts('troll: ' + rk);
		createComment(rk, troll);
	});
};

troll();
