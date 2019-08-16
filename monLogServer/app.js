var createError = require('http-errors');
var express = require('express');
var path = require('path');
var cookieParser = require('cookie-parser');
var logger = require('morgan');
var mongoOplog = require('mongo-oplog');
const oplog = mongoOplog('mongodb://127.0.0.1:27017/local', { ns: 'customers.customer' });

var AWS = require('aws-sdk');
AWS.config.update({ region: 'ap-south-1' });

oplog.tail();
var params = "";

oplog.on('insert', doc => {
  console.log(doc.o);
  var msg = "Hi Admin, \n\n New Customer is added : \n\n";
  params = {
    Message: msg + JSON.stringify(doc.o),
    TopicArn: 'arn:aws:sns:ap-south-1:143120903104:tricon'
  };

  // Create promise and SNS service object
  var publishTextPromise = new AWS.SNS({ apiVersion: '2010-03-31' }).publish(params).promise();

  // Handle promise's fulfilled/rejected states
  publishTextPromise.then(
    function (data) {
      console.log("Message:\n "+params.Message+" send sent to the topic "+params.TopicArn);
      console.log("MessageID is " + data.MessageId);
    }).catch(
      function (err) {
        console.error(err, err.stack);
      });
});

oplog.on('update', doc => {
  console.log(doc.o);
  var msg = "Hi Admin, \n\n A customer data is modified : \n\n";
  params = {
    Message: msg + JSON.stringify(doc.o),
    TopicArn: 'arn:aws:sns:ap-south-1:143120903104:tricon'
  };

  // Create promise and SNS service object
  var publishTextPromise = new AWS.SNS({ apiVersion: '2010-03-31' }).publish(params).promise();

  // Handle promise's fulfilled/rejected states
  publishTextPromise.then(
    function (data) {
      console.log("Message:\n "+params.Message+" send sent to the topic "+params.TopicArn);
      console.log("MessageID is " + data.MessageId);
    }).catch(
      function (err) {
        console.error(err, err.stack);
      });
});

oplog.on('delete', doc => {
  console.log(doc.o);
  var msg = "Hi Admin, \n\n A Customer record is removed : \n\n";
  params = {
    Message: msg + JSON.stringify(doc.o),
    TopicArn: 'arn:aws:sns:ap-south-1:143120903104:tricon'
  };

  // Create promise and SNS service object
  var publishTextPromise = new AWS.SNS({ apiVersion: '2010-03-31' }).publish(params).promise();

  // Handle promise's fulfilled/rejected states
  publishTextPromise.then(
    function (data) {
      console.log("Message:\n "+params.Message+" send sent to the topic "+params.TopicArn);
      console.log("MessageID is " + data.MessageId);
    }).catch(
      function (err) {
        console.error(err, err.stack);
      });
});

var indexRouter = require('./routes/index');
var usersRouter = require('./routes/users');

var app = express();

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');

app.use(logger('dev'));
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

app.use('/', indexRouter);
app.use('/users', usersRouter);

// catch 404 and forward to error handler
app.use(function (req, res, next) {
  next(createError(404));
});

// error handler
app.use(function (err, req, res, next) {
  // set locals, only providing error in development
  res.locals.message = err.message;
  res.locals.error = req.app.get('env') === 'development' ? err : {};

  // render the error page
  res.status(err.status || 500);
  res.render('error');
});

module.exports = app;
