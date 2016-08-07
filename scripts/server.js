express = require('express');
mongoose = require('mongoose');
bodyParser = require('body-parser');
methodOverride = require('method-override');
//models = require('./models/database_schema')
var pythonShell = require('python-shell');

// Create the application.
var app = express();
app.set('port', (process.env.PORT || 3000));

// Add Middleware necessary for REST API's
app.use(bodyParser.urlencoded({extended: true}));
app.use(bodyParser.json());
app.use(methodOverride('X-HTTP-Method-Override'));

// CORS Support
app.use(function(req, res, next) {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  next();
});

// Connect to MongoDB
//mongoose.connect('mongodb://localhost:8000/');
//db=mongoose.connection;
//db.once('open', function() 
{

  // Load the models.
  //app.models = require('./models/index');
  //models.initialize_model();

  // Load the routes.
  // var routes = require('./routes');
  // _.each(routes, function(controller, route) {
  // 	app.use(route, controller(app, route));
  // });

  // var verify = require('./route/verify')
  // var signup = require('./route/signup')
  // var signin = require('./route/signin')
  // var coordt = require('./route/set_coordinate')
  // var msg = require('./route/calc_coordinate')
  // var uri = require('./route/uri')
  // app.post('/verify',verify.verify)
  // app.post('/signup',signup.signup)
  // app.post('/signin',signin.signin)
  // app.post('/resend',verify.resend)
  // app.post('/set_coordinate',coordt.set_coordinate)
  // app.post('/message',msg.message)
  // app.post('/uri',uri.uri)
  app.get('/hashtag/:id', function(req, res){
    console.log(req.params.id);
    var _initial = new Date();
    var options = {

        args: [3,req.params.id]
      };

    pythonShell.run('my_script.py', options, function(err, results){

      console.log('results: %j', results);
      res.json(results);
      var _final = new Date();
      var milliseconds = (_final.getTime() - _initial.getTime());
      console.log(milliseconds)
    });

  });

  app.get('/mention/:id', function(req, res){
    console.log(req.params.id);
    var options = {

        args: [4,req.params.id]
      };

    pythonShell.run('my_script.py', options, function(err, results){

      console.log('results: %j', results);
      res.json(results);

    });

  });

  app.get('/popularmention/:id', function(req, res){
    console.log(req.params.id);
     var _initial = new Date();
    var options = {

        args: [2,req.params.id ]
      };

    pythonShell.run('my_script.py', options, function(err, results){

      console.log('results: %j', results);
      res.json(results);
      var _final = new Date();
      var milliseconds = (_final.getTime() - _initial.getTime());
      console.log(milliseconds)
    });

  });

  app.get('/popularhashtag/:id', function(req, res){
    console.log(req.params.id);
     var _initial = new Date();
    var options = {

        args: [1, req.params.id]
      };

    pythonShell.run('my_script.py', options, function(err, results){

      console.log('results: %j', results);
      res.json(results);
      var _final = new Date();
      var milliseconds = (_final.getTime() - _initial.getTime());
      console.log(milliseconds)

    });

  });

  app.get('/cooccurring/:id', function(req, res){
    console.log(req.params.id);
    var options = {
        mode: 'json',
        args: [5, req.params.id]
      };

    pythonShell.run('my_script.py', options, function(err, results){

      console.log('results: %j', results);
      res.json(results);

    });

  });

  app.get('/cooccurring_bfs/:id/:depth*?', function(req, res){
    console.log(req.params.id);

    if(req.params.depth === undefined)
      req.params.depth = 2;
    var options = {
        mode: 'json',
        args: [6, req.params.id, req.params.depth]
      };

    pythonShell.run('my_script.py', options, function(err, results){

      console.log('results: %j', results);
      res.json(results);

    });

  });

  console.log('Listening on port...' + app.get('port'));
  app.listen(app.get('port'),'0.0.0.0');
};

