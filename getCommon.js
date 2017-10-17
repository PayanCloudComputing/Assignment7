'use strict';

const HTTPS = require('https');
const CryptoJS = require('crypto-js');
const Fort = require('string_format');
const Async = require('async');
const Promise = require('bluebird');
const AWS = require('aws-sdk');
const Lambda = new AWS.Lambda({'region': 'us-east-1'});
const S3 = new AWS.S3({'region': 'us-east-1'});

let apiKey = 'b0560a548f2907a88e6395e82d0beb5c';
let privateKey = '46bfda5361fd5b7c9420498f7a490a8a80237043';
let ts = new Date().getTime();
let baseEndPoint = 'https://gateway.marvel.com/';
let hash = CryptoJS.MD5(ts + privateKey + apiKey).toString();
let comicsURL = baseEndPoint + 'v1/public/characters/{0}/comics?apikey={1}&ts={2}&hash={3}&limit={4}&offset={5}';
let seriesURL = baseEndPoint + 'v1/public/characters/{0}/series?apikey={1}&ts={2}&hash={3}&limit={4}&offset={5}';

module.exports.get = (event, context, callback) => {
  let char1 = Number(event.query.char1);
  let char2 = Number(event.query.char2);
  let bucket = 'payan-marvel';  
  let objectName = (char1 < char2)
  ? char1.toString() + '_' + char2.toString()
  : char2.toString() + '_' + char1.toString();
  

  listObjects({Bucket: bucket, Prefix: objectName})
  .then(function(data) {
    let objectExists = false;
    if(data.Contents.length) {
      objectExists = true
    }
    return objectExists;
  }).then(function(objectExists) {
    if(objectExists) {
      getObject({Bucket: bucket, Key: objectName})
      .then(function(data) {
        let response = JSON.parse(data.Body.toString());
        callback(null, response);
      });
    } else {
      let totalComics1;
      let totalComics2;
      let totalSeries1;
      let totalSeries2;
      Promise.join(
        get(comicsURL.format(char1, apiKey, ts, hash, 1, 0)),
        get(comicsURL.format(char2, apiKey, ts, hash, 1, 0)),
        get(seriesURL.format(char1, apiKey, ts, hash, 1, 0)),
        get(seriesURL.format(char2, apiKey, ts, hash, 1, 0)),
        function(comics1Res, comics2Res, series1Res, series2Res) {
          totalComics1 = comics1Res['data']['total'];
          totalComics2 = comics2Res['data']['total'];
          totalSeries1 = series1Res['data']['total'];
          totalSeries2 = series2Res['data']['total'];
        }
      ).then(function() {
        let tasksComics1 = makeTasks(char1, totalComics1, true);
        let tasksComics2 = makeTasks(char2, totalComics2, true);
        let tasksSeries1 = makeTasks(char1, totalSeries1, false);
        let tasksSeries2 = makeTasks(char2, totalSeries2, false);
    
        Promise.join(
          parallel(tasksComics1),
          parallel(tasksComics2),
          parallel(tasksSeries1),
          parallel(tasksSeries2),
          function(dataComics1, dataComics2, dataSeries1, dataSeries2) {
            let comics1 = getItems(dataComics1);
            let comics2 = getItems(dataComics2);
            let commonComics = intersect(comics1, comics2);

            let series1 = getItems(dataSeries1);
            let series2 = getItems(dataSeries2);
            let commonSeries = intersect(series1, series2);

            let response = buildResponse(commonComics, commonSeries);
            let body = new Buffer.from(JSON.stringify(response));
            putObject({Bucket: bucket, Key: objectName, Body: body});

            callback(null, response);
          }
        );
      })
    }
  }).catch(function(error) {
    console.log(error, error.stack);
  });
}

var listObjects = Promise.method(function(params) {
  return new Promise(function(resolve) {
    S3.listObjects(params, function(error, data) {
      if (error) console.log(error, error.stack);
      else resolve(data);
    });
  });
});

var getObject = Promise.method(function(params) {
  return new Promise(function(resolve) {
    S3.getObject(params, function(error, data) {
      if (error) console.log(error, error.stack);
      else resolve(data);
    });
  });
});

var putObject = Promise.method(function(params) {
  return new Promise(function(resolve) {
    S3.putObject(params, function(error, data) {
      if (error) console.log(error, error.stack);
      else resolve(data);
    });
  });
});

var get = Promise.method(function(url) {
  return new Promise(function(resolve) {
    HTTPS.get(url, (response) => {
      response.setEncoding('utf8');
      let totalData = '';
      response.on('data', (data) => {
        totalData += data;
      });
      response.on('end', (data) => {
        let res = JSON.parse(totalData);
        resolve(res);
      });
    })
  })
});

var parallel = Promise.promisify(Async.parallel);

function makeTasks(id, total, isComic) {
  let iterations = Math.ceil(total/100);
  let tasks = [];
  let lambda = (isComic)
    ? 'payan-marvel-service-dev-get-character-comics-chunk'
    : 'payan-marvel-service-dev-get-character-series-chunk';
    
  for (let index = 0; index < iterations; index++) {
    let offset = index * 100;
    tasks.push(function(callback){
      let lambdaParams = {
        FunctionName : lambda,
        InvocationType : 'RequestResponse',
        Payload: '{ "offset": ' + offset + ', "id": ' + id + '}'
      };
      Lambda.invoke(lambdaParams, function(error, data){
        if(error){
          callback(error);
        }
        else{
          callback(null, data);
        }
      });
    });
  }
  return tasks;
}

function getItems(data) {
  let items = [];
  for (let index = 0; index < data.length; index++) {
    items.push.apply(items, JSON.parse(data[index].Payload));
  }
  return items;
}

function intersect(array1, array2) {
  var intersection = [];
  while(array1.length > 0 && array2.length > 0) {
    if (array1[0] < array2[0] ) {
      array1.shift();
    } else if (array1[0] > array2[0] ) {
      array2.shift();
    } else {
      intersection.push(array1.shift());
      array2.shift();
    }
  }
  return intersection;
}

function buildResponse(comics, series) {
  return {'comics': comics, 'series': series};
}
