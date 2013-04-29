
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
var limit = process.env.FIX_DB_LIMIT || 1000

var superagent = require('superagent')
var server = process.env.COUCHDB_HOST || 'localhost'
var port = process.env.COUCHDB_PORT || 5984
var user = process.env.COUCHDB_USER
var pass = process.env.COUCHDB_PASS


var couchdb = 'http://'+server+':'+port

var async = require('async')
var _=require('lodash')
var created = false
var done = false

function _init(cb){
    // make sure the tracking db is created
    // and any other init tasks
    if(!created){

        superagent.put(couchdb+'/'+statedb)
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


function what_to_get(db,state,cb){
    if(done) return cb(null)
    // build the query
    var query = {limit:limit}
    if(state){
        query.startkey=state
    }
    return cb(null,db,query)
}

var doclist=[]
function load_list(db,query,cb){
    if(doclist.length) return cb(null,db)
    query.db = db
    console.log(query)
    viewer(query,function(e,r){
        if(e) throw new Error(e)
        if(!r) throw new Error('no response from couchdb')
        // load all thet docs into the list
        console.log('got '+r.rows.length+' docs')
        console.log('extract the doc ids')
        doclist = _.map(r.rows
                       ,function(row){
                            return row.id
                        })
        return cb(null,db)
    })
    return null
}


function increment_counter(db,next_doc,cb){
    console.log(Date.now())
    couch_set({'db':statedb
              ,'doc':db
              ,'state':'fetched'
              ,'value':next_doc
              }
             ,cb)
    return null
}

function get_it(db,cb){
    if(!doclist.length) return cb('done')

    var id = doclist.shift()
    //console.log(doclist.length)
    superagent.get(couchdb + '/'+db+'/'+id)
    .type('json')
    .set('accept','application/json')
    .end(function(e,r){

        if(e || !r ){
            console.log('error fetching '+id)
            // so skip this one
            // have to sleep for 5 seconds for couchdb to start up again, to be safe
            var next_doc = doclist[0]
            console.log('stalling to wait for couchdb to come back up')
            console.log(Date.now())
            setTimeout(function(){
                increment_counter(db
                                 ,next_doc
                                 ,function(){
                                      console.log('done handling error')
                                      return cb(null,[])
                                  }
                                 )
            }
                      , 10000)
            return null
        }

        // r is the json response from db

        var next_doc = doclist[0]
        var docs = [r.body]
        increment_counter(db,next_doc,function(e,r){
            if(e){
                throw new Error (e)
            }
            // send back the results
            var datadocs = _.filter(docs
                                   ,function(row){
                                        //console.log(row)
                                        if(!row){
                                            return false
                                        }
                                        if(row.data !== undefined){
                                            return true
                                        }
                                        return false
                                    })
            _.each(datadocs
                  ,function(doc){
                       // replace the stupid id with a better one
                       var obs1 = doc.data[0]
                       var ts = obs1.ts
                       var lane = obs1.lane
                       var vds_id = obs1.vds_id || obs1.site_dir
                       doc._id = [vds_id,ts,Math.floor(Math.random()*1000) ].join('-')
                   });
            cb(null,datadocs)
        })
        return null
    })
    return null
}

function get_docs(db,cb){
    if(done) return cb(null)
    var operations = [function(cb2){
                          // load the stack with the db
                          return cb2(null,db)
                      }
                     ]

    if(!created){
        // first time through make sure tracking db has been created
        operations.unshift(_init)

        // and

        // load up the huge list of docs
        operations.push(where_are_we)
        operations.push(what_to_get)
        operations.push(load_list)
    }
    operations.push(get_it)
    async.waterfall(operations
                   ,function(e,result){
                        if(e){
                            if(e === 'done') return cb(e)
                            throw new Error(e)
                        }
                        if(!result || result.length===0){
                            console.log('no docs')
                            if(doclist.length) return cb('try again')
                        }
                        console.log('all good')
                        return cb(null,result)
                    })
    return null
}
module.exports=get_docs
