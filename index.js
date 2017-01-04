var express = require('express');
var router = express.Router();

/* GET home page. */
router.get('/', function(req, res, next) {
	console.log("####### RENDER DASH BOARD #########");
  res.render('index', { message: 'Express' });
});

router.get('/conn', function(req, res) { 

});

router.get('/connect', function(req, res) {
    console.log("### INSIDE CONNECTION TO HIVE ###");
    var hive = require('thrift-hive');
    var thrift = require('thrift');
    transport = thrift.TBufferedTransport();
    protocol = thrift.TBinaryProtocol();

    var client = hive.createClient({
        version: '0.7.1-cdh3u2',
        server: '10.0.66.240',
        port: 9083,
       // username:'dev_Rht',
       // password:'dev@1234',
        timeout: 1000
    });
    // Execute call 
    client.execute('use default', function(err) {
        client.query('show tables')
            .on('row', function(database) {
                console.log("########### CONNECTED SUCCESSFULL #####");
                console.log(database);
            })
            .on('error', function(err) {
                console.log("######### ERROR ########## "+err)
                console.log(err.message);
                client.end();
            }).on('end', function() {
                console.log('########## END ###########')
                client.end();
        });
    });
});

router.get('/connect2', function(req, res) {
    console.log("######## connect2 ########## ");

    var thrift = require('thrift');
    transport = thrift.TBufferedTransport();
    protocol = thrift.TBinaryProtocol();

    var hive = require('node-hive').for({
        server: "hive.myserver"
    });

    hive.fetch("SELECT * FROM my_table", function(err, data) {
        if (err) {
            console.log("######## RROR ###########")
        } else {
            data.each(function(record) {
                console.log(record);
            });
        }
    });
})

// connect const method
router.get('/con', function(req, res) {
    console.log("######## con vinayak called ########## ");
    ////// SET TIMEOUT STARTS
		function f() {		
   
// const test start
const exec = require('child_process').exec;
    var spawn = require('child_process').spawn;
    /*exec('sqlldr admin/12345@localhost:1521/xe control=csvloader.txt log= csvloader.log', (err, stdout, stderr) => {
      if (err) {
        console.error(err);
        return;
      }
      console.log(stdout);
    });*/

   // using spwan big_query/axis#1234@10.9.112.237:44106/AXBIGQ
    //const sqlldr = spawn('sqlldr', ['admin/12345@localhost:1521/xe control=csvloader.txt log=csvloader.log']);
  


     
     const hive = spawn('hive', ['-f'+'./test.hql']);

    hive.stdout.on('data', (data) => {
       console.log("## COMMND XECUTED Success :" + ` ${data}`);
       var newarr = [];
       console.log("Data : "+data.toString());
      var _d=data.toString();
        //  global._global=_d;
          //console.log("########THIS HHAS BEEN CHANGED TOOOOOOOOOOOOO: "+_global +"***"+ _d);    
 //  var _g=global._d;
      //global._d;
      //res.send(data.toString());
	  console.log("STRINGIFY RESULTS:  "+JSON.stringify(_d));
	  res.send(JSON.stringify(_d));
     });

    
        //utility.sendError(err);

////// ERRROR START
hive.stderr.on('data', (data) => {
        console.log("## COMMND ERROR ## : " + `stderr: ${data}`);
 });
////// ERROR END
			console.log("TIME OUT 11111")}
			setTimeout(f, 6) 
			console.log("******TIME OUR @@@@@@@@@@@@");
   ////////////// SET TIMEOUT ENDS
   //res.send(_d);
   console.log("THIS SHOULD BE THE GLOBAL DATA:   "+_global);
                    
// const test end

})

//************************************************************************

router.get('/con2', function(req, res) {
    console.log("*** CONN 2 CALLED****");
    // spawn start
    var exec = require('child_process').exec;

    var execute = function(command, callback) {
        exec(command, function(error, stdout, stderr) {
            callback(error, stdout);
        });
    };

    execute("hive -f ./test.hql", function(err, json, outerr) {
            if (err) throw err;
            //console.log("*********FORMATTED DATA \n" + json);
	     //console.log("TYPE OF "+typeof(json)); 	

           //var _newJSON = JSON.parse(json);
           //console.log("********TYPE OF PARSED DATA "+json.split(' ')); 

	    var buf = new Buffer(json);
	    var finalJson = buf.toJSON(buf);

	   console.log("############## JSON ###############"+json);
      
        })
        // spawn ends


})

module.exports = router;
