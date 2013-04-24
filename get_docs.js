
/**
 * This module hits the database and gets chunks of docs.  It also
 * uses couchdb to store progress, so that other processes can also do
 * the job too
 */

var couch_check = require('couch_check_state')
var couch_set   = require('couch_set_state')
var viewer = require('couchdb_get_views')
var to_query = require('couchdb_toQuery')

var statedb = process.env.FIX_DB_STATE_DB || "breakup%2fbig%2fdbs"
var superagent = require('superagent')
var server = process.env.COUCHDB_HOST || 'localhost'
var port = process.env.COUCHDB_PORT || 5984
var user = process.env.COUCHDB_USER
var pass = process.env.COUCHDB_PASS

var async = require(async)

var couchdb = 'http://'+server+':'+port

var created = false
var done = false
function init(cb){
    // make sure the tracking db is created
    // and any other init tasks
    if(!created){
        superagent.put(statedb)
        .auth(user,pass)
        .end(function(e,r){
            created=true
            // I don't really care if it fails
            return cb()
        })
    }else{
        cb()
    }
    return null
}

function where_are_we(db,cb){
    // where are we supposed to be
    if(done){
        return cb(null)
    }
    couch_check({'db':statedb
                ,'doc':db
                ,'state':'fetched'
                }
               ,function(err,state){
                    if(err) throw new Error(err)
                    return cb(null,db,state)
                })
    return null
}
var limit = process.env.FIX_DB_LIMIT || 100
limit++

function what_to_get(db,state,cb){
    if(done) return cb(null)
    // build the query
    var query = {limit:limit
                ,include_docs:true
                }
    if(state!==undefined){
        query.startkey=state
    }
    cb(null,query)
}

function get_it(db,query,cb){
    if(done) return cb(null)
    query.db = db
    viewer(query,function(e,r){
        // r is the json response from db
        // need to save that last doc in the tracking db
        var rows = r.rows
        // if the rows length is the same as the limit, then there is
        // more to do, so remove the last doc from the set of returned
        // rows.  It will get processed with the next call
        var last_fetched
        done = rows.length < limit

        if(!done){
            last_fetched= rows.shift()
            last_fetched = last_fetched.key
        }else{
            last_fetched=rows[rows.length-1].key
        }

        couch_set({'db':statedb
                  ,'doc':db
                  ,'state':'fetched'
                  ,'value':last_fetched
                  }
                 ,function(e){
                      if(e) throw new Error (e)
                      // send back the results
                      cb(null,rows)
                  })
        return null
    })
    return null
}
function get_docs(db,cb){
    if(done) return cb(null)
    var operations = [function(cb2){
                    // load the stack with the db
                    return cb2(db)
                }
               ,where_are_we
               ,what_to_get
               ,get_it
               ]

    if(!created){
        // first time through make sure tracking db has been created
        operations.unshift(init)
    }
    async.waterfall(operations
                   ,function(e,result){
                        if(e) throw new Error(e)
                        cb(null,result)
                    })
    return null
}
