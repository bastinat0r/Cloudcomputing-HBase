var util = require('util');
var hbase = require('hbase');
var assert = require('assert');

hbase({ host: '127.0.0.1', port: 8080 })
	.getTable('hello_table' )
	.create('hello_column_family', function(err, success){
			this
			.getRow('hello_row')
			.put('hello_column_family:hello_column', 'hello', function(err, success){
					this.get('hello_column_family', function(err, cells){
							this.exists(function(err, exists){
//									assert.ok(exists);
									util.puts("yay");
							});
					});
			});
	});
