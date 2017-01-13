var request = require('request');
var moment = require('moment');
var schedule = require('node-schedule');
var googleAuth = require('google-oauth-jwt');
var stringify = require('csv-stringify');
var async = require('async');
var fs = require('fs');
var parse = require('csv-parse');
var oracledb = require('oracledb');
var uuid = require('uuid');

var ga_config = require('../config/ga_config');
var ods_config = require('../config/ods_config');
var utility = require('./utility');
var _endDate;
var uid;
var cuid;
//GOOGLE AUTHENTICATE 

exports.getToken = function(key,cb) {
try {

    //########## GA AUTHENTICATE ###################
    // INITIALIZATION
    var _accessToken;
    console.log("########## KEY ######### "+key);

    console.log("************************** GOOGLE AUTHENTICATION EXECUTED ****************************");

    // AUTHENTICATE WITH GOOGLE APIS CONSOLE. 

    googleAuth.authenticate({
        // use the email address of the service account, as seen in the API console
        email:ga_config.email,
        // use the PEM file we generated from the downloaded key
        keyFile:key,
        // specify the scopes you wish to access
        scopes: ['https://www.googleapis.com/auth/analytics.readonly']
    }, function(err, token) {
        if(err){
            console.log("#### Error while Google Auth ##### "+err);
            var err = "ERROR while Authenticating Google. Server Error - "+err;
            utility.saveErrorLogGA(err);
        }else{
        console.log("Access Token : ");
        _accessToken = token;
        cb(_accessToken);
        //console.log("############# TOKEN ############ "+_accessToken);
        }
    });
}catch(ex) {
    console.log("Exception In Token : " + ex);
    utility.saveErrorLogGA(ex);
}
};

//###################### CSV WRITE #####################

exports.generateCsv = function(rule,cb){
    console.log("########## INSIDE GENERATE CSV ############ "+JSON.stringify(rule));
        try {
       
        global._gaStatussave=0;
         //***** update status start ****
        var updateScheduleID = ga_rowID;
        console.log("THIS IS ROW ID: "+updateScheduleID);
        connection.query('UPDATE schedules SET status="start" WHERE id= ? ',updateScheduleID,function(err,result){
                if(err){
                    console.log(err);
                }
                else{
                    console.log("UPDATE ROW TO START ** EXECUTE START");
                }
            })
        //***** update end**********
/*        var d = new Date();
        d.setDate(d.getDate() - 1);
        var _startDate = moment(d).format('YYYY-MM-DD');
        var _endDate = moment(d).format('YYYY-MM-DD');*/
        //var _yesterdayDate = moment(d).format('DD-MM-YYYY');
        //var yesterDay = moment(d).add(-30, 'days').format('YYYY-MM-DD');
         var start_index = 1;
         console.log("********THIS IS rule:  "+JSON.stringify(rule));
        connection.query('SELECT * FROM schedules WHERE id =?',[rule.name], function(err, schedulesData) {
            if (err) {
                console.log("########### ERRO IN CSV ############ "+err);
                var err = "ERROR in CSV MYSQL. Server Error - "+err;
                utility.saveErrorLogGA(err);
            } else {
                connection.query('SELECT * FROM portal where portal_name=?', [schedulesData[0].portal], function(err, portalData) {
                    if (err) {
                        console.log("Error : "+err);
                        var err = "ERROR Authenticating Portal Name in MYSQL. Error -"+err;
                        utility.saveErrorLogGA(err);
                    } else {
                        var keypath = portalData[0].security_key;
                        var _gaIds = portalData[0].gaid;
                        //var _gaIds = 'ga:110774585';
                        var _metricsReport1 = schedulesData[0].metrics;
                        var _dimensionsReport1 = schedulesData[0].dimensions
                        var _maxResult = "10000";
                        var start_index = 1;
                        //var todate = schedulesData[0].to_date;
                        //var fromdate = schedulesData[0].from_date;
                        //var sqlquery = 'INSERT INTO accga (browser,continent,country,city,hostname,pagePathLevel1,users,newUsers,pageviews,pageLoadTime,transactions,sessionsPerUser)';
                        var sqlquery =schedulesData[0].sql_query;
                        var flag = false;

                        var d = new Date();
                        d.setDate(d.getDate() - 1);
                        var _startDate = moment(d).format('YYYY-MM-DD');
                        _endDate = moment(d).format('YYYY-MM-DD');
                        //var _startDate = moment(todate).format('YYYY-MM-DD');
                        //_endDate = moment(fromdate).format('YYYY-MM-DD');
                        uid = uuid.v1();
                        var filter = schedulesData[0].filter;
                        console.log("******************THIS IS FILTERRRR:  "+filter);
                        
                        var _url = 'https://www.googleapis.com/analytics/v3/data/ga?ids=' + _gaIds + '&start-date=' + _startDate + '&end-date=' + _endDate + '&metrics=' + _metricsReport1 + '&dimensions=' + _dimensionsReport1;
                        if(filter!=''){
                            console.log("FILTER IS NOT NULL");
                            _url = 'https://www.googleapis.com/analytics/v3/data/ga?ids=' + _gaIds + '&start-date=' + _startDate + '&end-date=' + _endDate + '&metrics=' + _metricsReport1 + '&dimensions=' + _dimensionsReport1 + '&filters=' + filter/*'ga:userType%3D%3DNew Visitor'*/;
                        }   
                        var _data = {};
                        utility.getToken(keypath,function(_accessToken) {
                            start_index = 1;
                            console.log("######### UTIL CALLED ########## " + _url);
                            request(_url + '&max-results=' + _maxResult + '&access_token=' + _accessToken + '&start-index=' + start_index, function(err, response, body) {
                                if (err) {
                                    console.log("### Error While Request ### " + err);
                                    //utility.sendError(err);
                                    var err = "ERROR while Authenticating Google"+err;
                                    utility.saveErrorLogGA(err);
                                } else {

                                    _data = JSON.parse(body);
                                    //console.log("####### FETCHED DATA ##### "+JSON.stringify(_data));
                                    console.log("####### TOTAL DATA ##### " + JSON.stringify(_data.totalResults));
                                    console.log("****** I AM HERE NOW 1******");
                                    global._totalResults = _data.totalResults;
                                    console.log("GLOBAL _totalResults:  "+_totalResults);
                                    
                                    if (_data.nextLink == undefined) {
                                        console.log("####### NO NEXT LINK ########### " + _data["rows"].length);
                                        
                                        stringify(_data["rows"], function(err, output) {
                                            fs.writeFile('CSV-DATA/' +uid+'-'+_endDate + '-' + start_index + '.csv', output, 'utf8', function(err) {
                                                if (err) {
                                                    console.log('Some error occured - '+err);
                                                    var err = "ERROR while Writing to CSV File,Space on Server May be less. Try flusing all CSV Data and run the Job again. Server Error- "+err;
                                                    utility.saveErrorLogGA(err);
                                                } else {

                                                    console.log('It\'s saved ! :' +uid+'-'+_endDate + '-' + start_index + '.csv');
                                                    //utility.insertIntoDb('CSV-DATA/' +_endDate + '-' + start_index + '.csv');                                        
                                                    utility.callSQLLoder('CSV-DATA/' +uid+'-'+_endDate + '-' + start_index + '.csv',sqlquery,flag);
                                                }
                                            });
                                        });                                        
                                    } else {
                                        flag=true;
                                        stringify(_data["rows"], function(err, output) {
                                            fs.writeFile('CSV-DATA/' +uid+'-'+ _endDate + '-' + start_index + '.csv', output, 'utf8', function(err) {
                                                if (err) {
                                                    console.log("Error occured while writing CSV Error is - "+err);
                                                    //utility.sendError(err);
                                                    var err = "Error occured while writing CSV Error is -"+err;
                                                    utility.saveErrorLogGA(err);
                                                } else {
                                                    console.log('It\'s saved ! :' +uid+'-'+_endDate + '-' + start_index + '.csv');
                                                    //utility.insertIntoDb('CSV-DATA/' +_endDate + '-' + start_index + '.csv');
                                                    utility.callSQLLoder('CSV-DATA/' +uid+'-'+_endDate + '-' + start_index + '.csv',sqlquery,flag);

                                                }
                                            });
                                        });
                                        recursiveWrite(_data.nextLink, _data, _accessToken, start_index,sqlquery);
                                        cb("Success");
                                    }
                                }
                            });
                        });
                    }
                })
            }
        });
    } catch (ex) {
        console.log("Exception : " + ex);
    }

}


function recursiveWrite(_url, fileData, token,start_index,sqlquery) {
    console.log("######## UUUUUUUURRRRRRRRRRRLLLLLLLLLLLLL NO CUSTOMMMMM ######## "+_url);
  
    start_index=start_index+10000;
    var _data = {};
    request(_url + '&max-results=10000'  + '&access_token=' + token + '&start-index=' + start_index, function(error, response, body) {
        if (error) {
            console.log("### Error While Request : " + error);
            var err = "ERROR while REQUEST, Please check the metrics & dimension combination. Error is -"+error;
            utility.saveErrorLogGA(err);
        } else {
            //console.log("########## HIT COUNT ############ ");
            //count++;
            console.log("THIS IS START INDEX:  "+start_index);
            _data = JSON.parse(body);
            //console.log("######## LINK ##### "+_data.query['start-index']);
            
            console.log("### NEXT LINK ## " + _data['nextLink']);
            var flag = true;

            stringify(_data["rows"], function(err, output) {
                fs.writeFile('CSV-DATA/' +uid+'-'+_endDate + '-' + start_index + '.csv', output, 'utf8', function(err) {
                    if (err) {
                        console.log('Some error occured - '+err);
                        var err = "ERROR while REQUEST, Please check the metrics & dimension combination. Error is -"+err;
                        utility.saveErrorLogGA(err);
                        //utility.sendError(err);
                    } else {
                        console.log('It\'s saved!'+uid+'-'+_endDate + '-' + start_index + '.csv');
                        //utility.insertIntoDb('CSV-DATA/' +_endDate + '-' + start_index + '.csv');
                        utility.callSQLLoder('CSV-DATA/' +uid+'-'+_endDate + '-' + start_index + '.csv',sqlquery,flag);
                    }
                });
            });
            if(_data.nextLink != undefined){
                 global._gaStatussave=1; //test
                recursiveWrite(_data.nextLink,_data,token,_data.query['start-index'],sqlquery);
            } 
             //status update logic 
                if(_data.nextLink==undefined){
                    console.log("****I AM CALLED******* GREATT");
                    global._gaStatussave=2;
                }
            ////////////////////              
        }
    });
};


//######### SQL LDR UTILITY ################

exports.callSQLLoder = function(csvfile, sqlquery,flag) {
    console.log("###### INSIDE SQL LOADER ###### "+sqlquery);
    try {

        fs.readFile('csvloader.txt', 'utf8', function(err, data) {
            if (err) {
                console.log("########### ERROR IN SQL LOADER ############# "+err);
                var err = "ERROR While Inserting Data Into ODS, Please check your Insert & ODS Query and Try Again. Error is -"+err;
                utility.saveErrorLogGA(err);
                //utility.sendError(err);
            } else {

                //replace insert into table
                var trail = 'TRAILING NULLCOLS ' + sqlquery.match(/\((.*?)\)/img);
                console.log("######### TRAIL ######### " + trail);
                var insert = sqlquery.split(/\s+/).slice(2, 3).join(" ");                

                if(flag==true){
                    var result = data.replace(/^.*(INFILE).*$/img, 'INFILE  ' + "'" + csvfile + "'").replace(/^.*(TRAILING).*$/img, trail).replace(/^.*(INTO).*$/img, 'INTO TABLE  ' + insert).replace(/^.*(insert).*$/img,'APPEND');
                }else{
                    var result = data.replace(/^.*(INFILE).*$/img, 'INFILE  ' + "'" + csvfile + "'").replace(/^.*(TRAILING).*$/img, trail).replace(/^.*(INTO).*$/img, 'INTO TABLE  ' + insert);
                }
                console.log("######### RESULT 1111 ##########" + result);
                fs.writeFile('csvloader.txt', result, 'utf8', function(err, retunData) {
                    if (err) {
                        console.log("############### Error In SQL LOADER ######### 2 : " + err);
                        //utility.sendError(err);
                        var err = "ERROR While Inserting Data Into ODS, Please check your Insert & ODS Query and Try Again. Error is -"+err;
                        utility.saveErrorLogGA(err);
                    } else {
                        console.log("Write Success");

                        const exec = require('child_process').exec;
                        var spawn = require('child_process').spawn;
                        /*exec('sqlldr admin/12345@localhost:1521/xe control=csvloader.txt log= csvloader.log', (err, stdout, stderr) => {
                          if (err) {
                            console.error(err);
                            return;
                          }
                          console.log(stdout);
                        });*/

                        //using spwan big_query/axis#1234@10.9.112.237:44106/AXBIGQ
                        //const sqlldr = spawn('sqlldr', ['admin/12345@localhost:1521/xe control=csvloader.txt log=csvloader.log']);
                        const sqlldr = spawn('sqlldr', [ods_config.oraclestring+' control=csvloader.txt log=csvloader.log']);

                        sqlldr.stdout.on('data', (data) => {
                            
                             console.log(" I AM HERE & the STATUS is in Successvv : "+_gaStatussave);
                            console.log("## COMMND XECUTED Success :" + `stdout: ${data}`);
        //********************************************** UPDATE GA STATUS LOGIC STARTS *****************************//
                       var totalres =_totalResults;
                       //dividing totalresults by 10,000

                          var num = totalres/10000;
                          console.log("***BELOW IS THE NUMBER***");
                          console.log(num);
                          //displaying results after decimal point
                          console.log(num.toString().split(".")[1]);
                           var _dStringTotal = num.toString().split(".")[1]; 
                       console.log("***********I AM IN LINE 288********");
                       //searching only for integer 
                       var _dString = data.toString().replace( /^\D+/g, '');
                       console.log(" THIS IS  ONLY THE COUNT:  "+_dString+" **THIS IS TOTAL RESULT COUNT LENGTH: "+totalres.toString().length);
                              
                        //Final update logic start
                           if(_gaStatussave==2||_gaStatussave=2){
                            console.log(" *****I AM INSIDE IF CONDITION _gaStatussave==2 ***");
                            console.log("_dStringTotal "+_dStringTotal+" _dString "+_dString);
                            var _finalstatus =_dStringTotal-_dString;
                            console.log(_finalstatus);
                                if(_finalstatus==0){
                                    console.log(" *** I FOUND A MATCH with COUNT, I SHOULD BE CALLED AT END***");
                                    //Update start
                                       var rowID = ga_rowID;
                                        connection.query('UPDATE schedules SET status ="done" WHERE id= ? ',rowID,function(err,result){
                                            if(err){
                                                console.log(err);
                                            }
                                            else{
                                                console.log("UPDATE ROW TO DONE");                                                
                                            }
                                        })
                                    //Update Ends
                                }//if final status 0

                           } //if _gaStatussave ends
                        //Final update logic ends

      //*********************************************** UPDATE GA LOGIC END ***************************************//
 
  //************************************* UPDATE GA STATUS LOGIC STARTS ******************

                              console.log(" I AM HERE & the STATUS is in Successvv1 : "+_gaStatussave);

                             var _dString = data.toString().replace( /^\D+/g, '');
                             if(_totalResults<10000){
                                var _finalstatus =_totalResults-_dString;
                                if(_finalstatus==0){
                                var rowID = ga_rowID;
                                connection.query('UPDATE schedules SET status ="done" WHERE id= ? ',rowID,function(err,result){
                                    if(err){
                                        console.log(err);
                                    }
                                    else{
                                        console.log("UPDATE ROW TO DONE");     

                                    }
                                })
                            }// if finalstATUS ends
                             } //if totalresults<10000 end
                              
        //************************************ UPDATE GA LOGIC ENDS ***********************************
                        });

                        sqlldr.stderr.on('data', (data) => {
                            console.log("## COMMND ERROR ## : " + `stderr: ${data}`);
                            /*console.log("*** THIS IS ERR DATA: "+data);*/
                            //utility.sendError(err);
                            var err = "ERROR While Inserting Data Into ODS, Please check your Insert & ODS Query and Try Again. Error is -"+data;
                            utility.saveErrorLogGA(err);
                        });
                    }//else ends

                    ///****************************
                        console.log("*****THIS IS THE LENGTH of c_data callSQLLoder OUTTTT: ");
                    ///***************************
                });
            }
        });
    } catch (ex) {
        console.log("Exception : " + ex);
        //utility.sendError(err);
    }
}

//****************************************************************************
//################## CUSTOM SCHEDULAR     ####################################
//****************************************************************************

exports.generateCsvCustom = function(custom,cb){
    console.log("########## INSIDE GENERATE CSV ############ "+JSON.stringify(custom));
        try {
             global._gaStatussave=0;
        //***** update status start ****
        var updateScheduleID = ga_rowID;
        console.log("THIS IS THE ROW ID: "+updateScheduleID);
        connection.query('UPDATE schedules SET status="start" WHERE id= ? ',updateScheduleID,function(err,result){
                if(err){
                    console.log(err);
                }
                else{
                    console.log("UPDATE ROW TO START ** EXECUTE START");
                }
            })
        //***** update end**********
        var start_index = 1;

        connection.query('SELECT * FROM schedules WHERE id =?',[custom.id], function(err, schedulesData) {
            if (err) {
                console.log("########### ERRO IN CSV ############ "+err)
            } else {
                connection.query('SELECT * FROM portal where portal_name=?', [schedulesData[0].portal], function(err, portalData) {
                    if (err) {
                        console.log("Error : "+err);
                    } else {
                        var keypath = portalData[0].security_key;
                        var _gaIds = portalData[0].gaid;
                        var _metricsReport1 = schedulesData[0].metrics;
                        var _dimensionsReport1 = schedulesData[0].dimensions
                        var _maxResult = "10000";
                        var start_index = 1;
                        var todate = custom.todate;
                        var fromdate = custom.fromdate;
                        var sqlquery =schedulesData[0].sql_query;
                        var flag = false;
                        var filter = custom.filter;
                        /*var d = new Date();
                        d.setDate(d.getDate() - 1);
                        var _startDate = moment(d).format('YYYY-MM-DD');
                        _endDate = moment(d).format('YYYY-MM-DD');*/
                        var _startDate =fromdate;// moment(todate).format('YYYY-MM-DD');
                        _endDate = todate;//moment(fromdate).format('YYYY-MM-DD');
                        cuid = uuid.v1();
                        console.log("***************FILTER:   "+filter);

                        //var _url = 'https://www.googleapis.com/analytics/v3/data/ga?ids=' + _gaIds + '&start-date=' + _startDate + '&end-date=' + _endDate + '&metrics=' + _metricsReport1 + '&dimensions=' + _dimensionsReport1;

                        var _url = 'https://www.googleapis.com/analytics/v3/data/ga?ids=' + _gaIds + '&start-date=' + _startDate + '&end-date=' + _endDate + '&metrics=' + _metricsReport1 + '&dimensions=' + _dimensionsReport1;
                        if(filter!=''){
                            console.log("FILTER IS NOT NULL");
                            _url = 'https://www.googleapis.com/analytics/v3/data/ga?ids=' + _gaIds + '&start-date=' + _startDate + '&end-date=' + _endDate + '&metrics=' + _metricsReport1 + '&dimensions=' + _dimensionsReport1 + '&filters=' + filter/*'ga:userType%3D%3DNew Visitor'*/;
                        }                        

                        console.log("THIS IS NEW URL: "+_url);
                        var _data = {};
                        utility.getToken(keypath,function(_accessToken) {
                            start_index = 1;
                            console.log("######### UTIL CALLED ########## " + _url);
                            request(_url + '&max-results=' + _maxResult + '&access_token=' + _accessToken + '&start-index=' + start_index, function(err, response, body) {
                                if (err) {
                                    console.log("### Error While Request ### " + err);
                                    //utility.sendError(err);
                                    var err = "ERROR While Requesting GA URL,Please check if Proper dimension parameters are taken. Error is - "+err;
                                  utility.saveErrorLogGA(err);
                                } else {

                                    _data = JSON.parse(body);
                                    //console.log("####### FETCHED DATA ##### "+JSON.stringify(_data));
                                    console.log("####### TOTAL DATA ##### " + JSON.stringify(_data.totalResults));
                                    console.log("****TOTAL DATA WITHOUT stringify: "+_data.totalResults);
                                    console.log(" **** I AM HERE NOW 2 *********");
                                    global._totalResults = _data.totalResults;
                                    console.log("GLOBAL _totalResults:  "+_totalResults);
                                    
                                    if (_data.nextLink == undefined) {
                                        console.log("####### NO NEXT LINK ########### " + _data["rows"].length);
                                        
                                        stringify(_data["rows"], function(err, output) {
                                            fs.writeFile('CSV-DATA/' +cuid+'-'+ _endDate + '-' + start_index + '.csv', output, 'utf8', function(err) {
                                                if (err) {
                                                    console.log('Some error occured - '+err);
                                                    //utility.sendError(err);
                                                    var err = "ERROR While Requesting GA URL,Please check if Proper dimension parameters are taken. Error is - "+err;
                                                     utility.saveErrorLogGA(err);
                                                } else {

                                                    console.log('It\'s saved ! :' +cuid+'-'+_endDate + '-' + start_index + '.csv');
                                                    //utility.insertIntoDb('CSV-DATA/' +_endDate + '-' + start_index + '.csv');                                        
                                                    utility.callSQLLoderCustom('CSV-DATA/'+cuid+'-'+_endDate + '-' + start_index + '.csv',sqlquery,flag);
                                                }
                                            });
                                        });                                        
                                    } else {
                                        flag=true;
                                        stringify(_data["rows"], function(err, output) {
                                            fs.writeFile('CSV-DATA/'+cuid+'-'+_endDate + '-' + start_index + '.csv', output, 'utf8', function(err) {
                                                if (err) {
                                                    console.log('Some error occured - '+err);
                                                    //utility.sendError(err);
                                                    var err = "ERROR While Requesting GA URL,Please check if Proper dimension parameters are taken. Error is - "+err;
                                                    utility.saveErrorLogGA(err);
                                                } else {
                                                    console.log('It\'s saved ! :'+cuid+'-'+_endDate + '-' + start_index + '.csv');
                                                    //utility.insertIntoDb('CSV-DATA/' +_endDate + '-' + start_index + '.csv');
                                                    utility.callSQLLoderCustom('CSV-DATA/'+ cuid +'-' +_endDate + '-' + start_index + '.csv',sqlquery,flag);

                                                }
                                            });
                                        });
                                        recursiveWriteCustom(_data.nextLink, _data, _accessToken, start_index,sqlquery);
                                        cb("Success");
                                    }
                                }
                            });
                        });
                    }
                })
            }
        });
    } catch (ex) {
        console.log("Exception : " + ex);
    }

}


function recursiveWriteCustom(_url, fileData, token,start_index,sqlquery) {
    console.log("######## UUUUUUUURRRRRRRRRRRLLLLLLLLLLLLL CUSTOMWRITEE ######## "+_url);
      
    start_index=start_index+10000;
    var _data = {};
    request(_url + '&max-results=10000'  + '&access_token=' + token + '&start-index=' + start_index, function(error, response, body) {
        if (error) {
            console.log("### Error While Request : " + error);
            var err = "ERROR While Requesting GA URL,Please check if Proper dimension parameters are taken. Error is - "+error;
                                  utility.saveErrorLogGA(err);
        } else {
            //console.log("########## HIT COUNT ############ ");
            //count++;
            _data = JSON.parse(body);
            //console.log("######## LINK ##### "+_data.query['start-index']);
            
            console.log("### NEXT LINK ## " + _data['nextLink']);
            var flag = true;

            stringify(_data["rows"], function(err, output) {
                fs.writeFile('CSV-DATA/'+cuid +'-'+ _endDate + '-' + start_index + '.csv', output, 'utf8', function(err) {
                    if (err) {
                        console.log('Some error occured - '+err);
                        //utility.sendError(err);
                        var err = "ERROR While Requesting GA URL,Please check if Proper dimension parameters are taken. Error is - "+err;
                                  utility.saveErrorLogGA(err);
                    } else {

                        console.log('It\'s saved!'+cuid+'-'+_endDate + '-' + start_index + '.csv');
                        //utility.insertIntoDb('CSV-DATA/' +_endDate + '-' + start_index + '.csv');
                        utility.callSQLLoderCustom('CSV-DATA/'+cuid+'-'+_endDate + '-' + start_index + '.csv',sqlquery,flag);
                    }
                });
            });
            console.log("### NEXT LINK vvvvvvvv## " + _data.nextLink);
            if(_data.nextLink!=undefined){
                   global._gaStatussave=1; //test
                 console.log(" **** SUCCESS SAVE LOGIC *****:"+_gaStatussave);
                 console.log("******* THIS IS START INDEX within undefined: "+start_index);
                recursiveWrite(_data.nextLink,_data,token,_data.query['start-index'],sqlquery);
               
            }     
            //status update logic 
                if(_data.nextLink==undefined){
                    /*var _gaStatussave2=2;
                    var _gaStatussave=_gaStatussave2;*/
                    global._gaStatussave=2;
                    console.log("*****NOW NEXT LINK IS undefined:  "+_gaStatussave);
                }
            ////////////////////       
        }
    });
};


//######### SQL LDR UTILITY ################
//added start_index
exports.callSQLLoderCustom = function(csvfile, sqlquery,flag) {
    console.log("###### INSIDE SQL LOADER ###### "+sqlquery);
    try {
        //added start_index
        fs.readFile('csvloader.txt', 'utf8', function(err, data) {
            if (err) {
                console.log("########### ERROR IN SQL LOADER ############# "+err);
                //utility.sendError(err);
                var err = "ERROR While Pushing Data To ODS, Please check your ODS Query and try again. Error is - "+err;
                                  utility.saveErrorLogGA(err);
            } else {

                //replace insert into table
                var trail = 'TRAILING NULLCOLS ' + sqlquery.match(/\((.*?)\)/img);
                console.log("######### TRAIL ######### " + trail);
                var insert = sqlquery.split(/\s+/).slice(2, 3).join(" ");                

                if(flag==true){
                    var result = data.replace(/^.*(INFILE).*$/img, 'INFILE  ' + "'" + csvfile + "'").replace(/^.*(TRAILING).*$/img, trail).replace(/^.*(INTO).*$/img, 'INTO TABLE  ' + insert).replace(/^.*(insert).*$/img,'APPEND');
                }else{
                    var result = data.replace(/^.*(INFILE).*$/img, 'INFILE  ' + "'" + csvfile + "'").replace(/^.*(TRAILING).*$/img, trail).replace(/^.*(INTO).*$/img, 'INTO TABLE  ' + insert);
                }
                console.log("######### RESULT 222##########" + result);
                //added start_index
                fs.writeFile('csvloader.txt', result, 'utf8', function(err, retunData) {
                    if (err) {
                        console.log("############### Error In SQL LOADER ######### 2 : " + err);
                        //utility.sendError(err);
                        var err = "ERROR While Pushing Data To ODS, Please check your ODS Query and try again. Error is - "+err;
                                  utility.saveErrorLogGA(err);
                    } else {
                        console.log("Write Success");

                        const exec = require('child_process').exec;
                        var spawn = require('child_process').spawn;
                        /*exec('sqlldr admin/12345@localhost:1521/xe control=csvloader.txt log= csvloader.log', (err, stdout, stderr) => {
                          if (err) {
                            console.error(err);
                            return;
                          }
                          console.log(stdout);
                        });*/

                        //using spwan big_query/axis#1234@10.9.112.237:44106/AXBIGQ
                        //const sqlldr = spawn('sqlldr', ['admin/12345@localhost:1521/xe control=csvloader.txt log=csvloader.log']);
                        const sqlldr = spawn('sqlldr', [ods_config.oraclestring+' control=csvloader.txt log=csvloader.log']);
                        
                        //var _count = 0;
                        sqlldr.stdout.on('data', (data) => {
                            console.log("## COMMND XECUTED Successvv :" + `stdout: ${data}`);

                            //********************************************** UPDATE GA STATUS LOGIC STARTS *****************************//
                       var totalres =_totalResults;
                       //dividing totalresults by 10,000

                          var num = totalres/10000;
                          console.log("***BELOW IS THE NUMBER***");
                          console.log(num);
                          //displaying results after decimal point
                          console.log(num.toString().split(".")[1]);
                           var _dStringTotal = num.toString().split(".")[1]; 
                       console.log("***********I AM IN LINE 288********");
                       //searching only for integer 
                       var _dString = data.toString().replace( /^\D+/g, '');
                       console.log(" THIS IS  ONLY THE COUNT:  "+_dString+" **THIS IS TOTAL RESULT COUNT LENGTH: "+totalres.toString().length);
                              
                        //Final update logic start
                           if(_gaStatussave==2){
                            console.log(" *****I AM INSIDE IF CONDITION _gaStatussave==2 ***");
                            console.log("_dStringTotal "+_dStringTotal+" _dString "+_dString);
                            var _finalstatus =_dStringTotal-_dString;
                            console.log(_finalstatus);
                                if(_finalstatus==0){
                                    console.log(" *** I FOUND A MATCH with COUNT, I SHOULD BE CALLED AT END***");
                                    //Update start
                                       var rowID = ga_rowID;
                                        connection.query('UPDATE schedules SET status ="done" WHERE id= ? ',rowID,function(err,result){
                                            if(err){
                                                console.log(err);
                                            }
                                            else{
                                                console.log("UPDATE ROW TO DONE");                                                
                                            }
                                        })
                                    //Update Ends
                                }//if final status 0

                           } //if _gaStatussave ends
                        //Final update logic ends

      //*********************************************** UPDATE GA LOGIC END ***************************************//
 

        //************************************* UPDATE GA STATUS LOGIC STARTS ******************

                              console.log(" I AM HERE & the STATUS is in Successvv1 : "+_gaStatussave);

                             var _dString = data.toString().replace( /^\D+/g, '');
                             if(_totalResults<10000){
                                var _finalstatus =_totalResults-_dString;
                                if(_finalstatus==0){
                                var rowID = ga_rowID;
                                connection.query('UPDATE schedules SET status ="done" WHERE id= ? ',rowID,function(err,result){
                                    if(err){
                                        console.log(err);
                                    }
                                    else{
                                        console.log("UPDATE ROW TO DONE");     

                                    }
                                })
                            }// if finalstATUS ends
                             } //if totalresults<10000 end
                              
        //************************************ UPDATE GA LOGIC ENDS ***********************************
                        });
                                                
                          
                            
                        sqlldr.stderr.on('data', (data) => {
                            console.log("## COMMND ERROR ## : " + `stderr: ${data}`);
                            console.log("*** THIS IS ERR DATA: "+data);
                            var err = "ERROR While Pushing Data To ODS, Please check your ODS Query and try again. Error is - "+data;
                                  utility.saveErrorLogGA(err);
                        });
                    }

                });
            }
        });
    } catch (ex) {
        console.log("Exception : " + ex);
        //utility.sendError(err);
    }
}

//################## CUSTOM SCHEDULAR END ####################################

//########## FUNCTION TO ODS PUSH ONE BY ONE  #####################

exports.insertIntoDb = function(file) {

    try {
        console.log('#################### INSERT CALLED #######################');

        oracledb.getConnection({
            user: "admin",
            password: "12345",
            connectString: "localhost:1521/XE"


        }, function(err, oraconnection) {
            if (err) {
                console.log("########## ERROR WHILE INSERT ############ "+err);
                //utility.sendError(err);
            } else {
                console.log("connection success")
                
                var inputFile = file; //'./CSV-DATA/mycsv.csv';
                var parser = parse({
                    delimiter: ','
                }, function(err, data) {
                    async.eachSeries(data, function(line, callback) {
                        var csvData = line.toString().split(",");
                        oraconnection.execute(
                            'INSERT INTO AXIS_CSV_1 (USERTYPE,BROWSER,USERS,NEWUSERS,PAGETITLE) VALUES (:1,:2,:3,:4,:5)', [csvData[0], csvData[1], csvData[3], csvData[4], csvData[2]], {
                                autoCommit: true
                            },
                            function(err, result) {
                                if (err) {
                                    console.error("ERROR :" + err.message);
                                    //utility.sendError(err);
                                    var err = "ERROR While Pushing Data To ODS, Please check your ODS Query and try again. Error is - "+err;
                                  utility.saveErrorLogGA(err);
                                } else {
                                    console.log("#### ROW INSERTED Success fully #### " + JSON.stringify(result));
                                    //console.log("######## connection ####### "+JSON.stringify(oraconnection));
                                    //cb("row inserted success !");
                                    //oraconnection.close();
                                    //oraconnection.release();
                                    
                                }
                            });
                        callback();
                    })
                });
                fs.createReadStream(inputFile).pipe(parser);
            }
        });
    } catch (ex) {
        console.log("Exception : " + ex);
        //utility.sendError(err);
    }
}


//############ SEND EMAIL To SEND ERROR REPORT #############################################3
/*var nodemailer = require('nodemailer');

exports.sendError = function(errData){
    console.log("####### EMAIL TEXT ####### "+errData);
    var data = errData;
    //var transporter = nodemailer.createTransport('smtp://arjun@appliedcloudcomputing.com:arjunacc@smtp.gmail.com');
}
=======
   res.send(data);
}*/

//#################### ERROR LOG INSERT #####################################3
exports.saveErrorLog = function(data,url){
    //id|error|line no|function name|date|time
    console.log("********* ERROR LOG SaVED***********");
    console.log("~~~~~~~~~~~~~ EXCEPTION ~~~~~~ "+data);
    //console.log("~~~~~~~~~~~~~ EXCEPTION ~~~~~~ "+data.message);
    console.log("~~~~~~~~~~~~~ EXCEPTION ~~~~~~ "+url);

    var datetime = moment(new Date()).format('DD/MM/YYYY hh:mm:ss a') ;// moment().format('L'); //YYYY-MM-DD HH:MM:SS
    //var time = moment().format('LT');  
    //var msg = data;
    //console.log("~~~~~~~~~~~~~~~~~ TIME "+datetime);
    jsonData = {
        'datetime':datetime,
        'message':data,
        'url':url
    }
  
    connection.query('INSERT INTO errolog SET ? ',jsonData,function(err,result){
        if(err){
            console.log("###### ERROR ##########"+err.message);
            console.log("###### ERROR ##########"+JSON.stringify(err));
        }else{
            console.log('~~~~~~~~~~~~ LOG SAVED Success ~~~~~~~~~~~~~');
            //res.end();
            console.log("THIS IS THE ROW ID "+bq_rowID);
            var rowID = bq_rowID;
            connection.query('UPDATE bqschedules SET fail ="fail" WHERE id= ? ',rowID,function(err,result){
                if(err){
                    console.log(err);
                }
                else{
                    console.log("UPDATE ROW");
                }
            })            
           
            
        }
    });

}

//**********************************GA SUCCESS MESSAGE START***************************//


//**********************************GA SUCCESS MESSAGE ENDS***************************// 

//#################### GA ERROR LOG INSERT #####################################3
exports.saveErrorLogGA = function(data,url){
    //id|error|line no|function name|date|time

    console.log("~~~~~~~~~~~~~ EXCEPTION ~~~~~~ "+data);
    //console.log("~~~~~~~~~~~~~ EXCEPTION ~~~~~~ "+data.message);
    console.log("~~~~~~~~~~~~~ EXCEPTION ~~~~~~ "+url);

    var datetime = moment(new Date()).format('DD/MM/YYYY hh:mm:ss a') ;// moment().format('L'); //YYYY-MM-DD HH:MM:SS
    //var time = moment().format('LT');  
    //var msg = data;
    //console.log("~~~~~~~~~~~~~~~~~ TIME "+datetime);
    jsonData = {
        'datetime':datetime,
        'message':data,
        'url':url
    }

    connection.query('INSERT INTO errlogga SET ? ',jsonData,function(err,result){
        if(err){
            console.log("###### ERROR ##########"+err.message);
            console.log("###### ERROR ##########"+JSON.stringify(err));
        }else{
            console.log('~~~~~~~~~~~~ LOG SAVED Success ~~~~~~~~~~~~~');
           // return false;
           //res.send("ERROR Saved");
           //res.end();
         //  res.json("error");
         console.log("THIS IS THE ROW ID "+ga_rowID);
            var rowID = ga_rowID;
            connection.query('UPDATE schedules SET status ="fail" WHERE id= ? ',rowID,function(err,result){
                if(err){
                    console.log(err);
                }
                else{
                    console.log("UPDATE ROW");
                }
            })
        }
    });

}
