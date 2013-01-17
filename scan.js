var util = require('util');
var hbase = require('hbase');

var commentNumber = 0;
var tablename = 'location';

// create table if !exists
var hb = hbase({ host: '127.0.0.1', port: 8090 });
var table = hb.getTable(tablename); 

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
var sc = table.getScanner();

/*
table.create('cf', function(err, foo) {
	if(err)
		util.puts(err);
});

sc.create({
	batch : 10,
  filter: {
    "op":"MUST_PASS_ALL","type":"FilterList","filters":[{
        "op":"EQUAL",
        "type":"RowFilter",
        "comparator":{"value":"DE_BERLIN.+","type":"RegexStringComparator"}
      }, {
				"op":"EQUAL",
				"type" : "QualifierFilter",
				"comparator" : { "value" : "name", "type" : "RegexStringComparator"}
			}
    ]}
}, function(error, cells){
	if(error)
		util.puts(error);
	if(cells) {
		util.puts(cells);
		this.get(scCallback);
	}
});
*/

table.create('comment', function(err, foo) {
	if(err)
		util.puts(err);
});

sc.create({
	batch : 2,
  filter: {
    "op":"MUST_PASS_ALL","type":"FilterList","filters":[{
        "op":"EQUAL",
        "type":"RowFilter",
        "comparator":{"value":"DE_BERLIN.+","type":"RegexStringComparator"}
      }, {
				"op":"EQUAL",
				"type" : "QualifierFilter",
				"comparator" : { "value" : 'author0' , "type" : "RegexStringComparator"}
			}
    ]}
}, function(error, cells){
	if(error)
		util.puts(error);
	if(cells) {
		util.puts(cells);
		this.get(scCallback);
	}
});

function fetchAllComments(cells, fn) {
	if(cells.length <= 0 && fn != null) {
		fn();
	} else {
		fetchComment(cells.shift().key, function() {
			fetchAllComments(cells, fn);
		});
	}
}

function scCallback(err, cells) {
	if(err)
		util.puts(err);
	if(cells) {
		util.puts('\n');
		fetchAllComments(cells, function() {
			sc.get(scCallback);
		});
	}
};

