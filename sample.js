var assert = require('assert');
var hbase = require('hbase');
var util = require('util');

hbase({ host: '127.0.0.1', port: 8090 })
.getTable('my_table' )
.create('my_column_family', function(err, success){
		this
		.getRow('my_row')
		.put('my_column_family:my_column', 'my value', function(err, success){
				this.get('my_column_family', function(err, cells){
						this.exists(function(err, exists){
								if(err) {
									util.puts(err);
								}
								assert.ok(exists);
						});
				});
		});
});
