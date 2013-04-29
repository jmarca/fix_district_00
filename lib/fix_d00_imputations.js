/* global process require console __dirname */

/**
 * the point of this program is to fix the d00 database by assigning
 * each document to the correctly named database
 */

// set options by environment variables
var env = process.env;

var async = require('async')

var argv = require('optimist')
    .usage('Fix a large couchdb of imputations by splitting into smaller DBs, sharded by district and by year.\nUsage: $0')
    .default('db','d00')
    .alias('db', 'fixdb')
    .describe('db', 'The really big imputations DB that you want to break up.')
    .default('t','vdsdata%2fbreakup%2ftracking')
    .alias('t', 'trackingdb')
    .describe('t', 'A CouchDB database that holds the current state of the break up process, in case it crashes and you have to restart.  Not really suitable for parallel processing, though')
    .argv
;
var district = argv.db
var tracking = argv.t

// logic.  loop until all files have been fetched from this monstrous db

// store my current place in the tracking db, so that I can crash and
// start over again

// because jeez, 100G is a lot

// start with 100 docs at a time, ramp up from there.


// set env vars
process.env.FIX_DB_STATE_DB=tracking
var prefix='beta%2fimputed%2fbreakup'
process.env.FIX_DB_TARGET_PREFIX =prefix

// load libs
var get_docs = require('./get_docs')
var doc_process = require('./doc_processor')
var saver = doc_process.save_stash
var processor = doc_process.doc_process


var processed=100
var opstack = [function(cb){
                   return cb(null,district)
               }
               // get 100 or so documents

              ,get_docs
               // process those docs
              ,function(docs,cb){
                   
                   processed = docs.length
                   if(!processed) return cb('done')
                   async.eachLimit(docs,2
                                  ,processor
                                  ,function(e,r){
                                       return cb(null,docs)
                                   })
                   return null
               }
              ,function(docs,cb){
                   saver(function(e){
                       if(e) throw new Error(e)
                       return cb(null,docs)
                   })
               }
              //,deleter
              ]

function doit(cbw){
    async.waterfall(opstack
                   ,function(e){
                        // done
                        if(e && e === 'try again'){
                            processed=1
                            return cbw(null)
                        }
                        return cbw(e)
                    });
    return null
}

var loops = 0
async.whilst(function(){
    return processed > 0
},function(next_loop){
      async.series([doit]
                  ,function(e){
                       console.log('looping '+loops++)
                       return next_loop(e)
                   })
      return null
  },function(e){
        if(e && e !=='done'){
            console.log(e)
        }else{
            console.log('done as doorknobs')
        }
        return null
    })
console.log('next tick the magick happens')
