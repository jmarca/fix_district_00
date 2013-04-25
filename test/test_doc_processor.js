/* global require console process describe it */

var should = require('should')

var _ = require('lodash')
var async = require('async')
var superagent = require('superagent')

var couch_check = require('couch_check_state')
var make_saver = require('couchdb_bulkdoc_saver')

var env = process.env;
var cuser = env.COUCHDB_USER ;
var cpass = env.COUCHDB_PASS ;
var chost = env.COUCHDB_HOST || 'localhost';
var cport = env.COUCHDB_PORT || 5984;

// any db with something in it
// I should probably create this db

var test_db = env.TEST_GET_DOCS_DB

var tracking_db ='test%2ftracker%2f'+Math.floor(Math.random() * 100)

var couch = 'http://'+chost+':'+cport


//must set the env var before loading and creating the getter
process.env.FIX_DB_STATE_DB=tracking_db
//must set the env var before loading and creating the getter
var prefix='test%2fblah%2fblah'
process.env.FIX_DB_TARGET_PREFIX =prefix

var fs = require('fs')

function lame(file,fn){
    fs.readFile(file,function(err,d){
        if(err){console.log('die')
                console.log(err)
                throw new Error(err)}
        var doc = JSON.parse(d)
        return fn(null,JSON.parse(d))
    })
}
var path = require('path')
var filepath = path.normalize(__dirname+'/files/wimdoc.json')

var get_docs = require('../lib/get_docs')
var doc_process = require('../lib/doc_processor')
var saver = doc_process.save_stash
var processor = doc_process.doc_process

var created_locally=false
var cleardbs=[]

before(function(done){
    superagent.put(couch+'/'+tracking_db)
    .type('json')
    .auth(cuser,cpass)
    .end(function(r,e){
        if(test_db !== undefined) return done()

        // create a test db, the put data into it
        test_db ='test%2fgetter%2f'+Math.floor(Math.random() * 100)
        created_locally=true
        var bulks = make_saver(test_db)
        // generate some random docs
        var docs=[]
        for(var i=1;i<1000;i++){
            var vdsid = ['11320',Math.floor(Math.random()*100)].join('')
            var ts='2007-02-01 23:23:00'
            docs.push({'_id':i+'_doc'
                      ,'data':[{vdsid:vdsid
                               ,ts:ts}
                              ,{vdsid:vdsid
                               ,ts:ts}
                              ]
                      })
        }

        superagent.put(couch+'/'+test_db)
        .type('json')
        .auth(cuser,cpass)
        .end(function(e,r){
            r.should.have.property('error',false)
            if(!e)
                created_locally=true
            // now populate that db with some docs
            bulks({docs:docs},function(e,r){
                if(e) done(e)
                _.each(r
                      ,function(resp){
                           resp.should.have.property('ok')
                           resp.should.have.property('id')
                           resp.should.have.property('rev')
                       });
                return done()
            })
            return null
        })
        return null
    })
})

after(function(done){
    var uri = 'http://'+chost+':'+cport+'/'+tracking_db
    // bail in development
    //console.log({tracking_db:tracking_db
    //            ,test_db:test_db})
    //return done()
    cleardbs.push(tracking_db)
    cleardbs.push(test_db)
    async.each(cleardbs
              ,function(db,cb){
                   superagent.del(couch+'/'+db)
                   .type('json')
                   .auth(cuser,cpass)
                   .end(function(e,r){
                       if(e) return done(e)
                       return cb()
                   })
                   return null
               }
              ,done);
    return null
})


describe('process docs',function(){

    it('should get some docs and process them'
      ,function(done){
           async.eachSeries([1,2,3,4,5,6,7,8,9]
                           ,function(i,cb){
                                get_docs(test_db,function(e,docs){
                                    if(e) return cb(e)
                                    _.each(docs
                                          ,function(resp){
                                               resp.should.have.property('key')
                                               resp.should.have.property('doc')
                                               resp.doc.should.have.property('_id')
                                           });
                                    docs.should.have.property('length',100)
                                    // check the tracking db
                                    couch_check({'db':tracking_db
                                                ,'doc':test_db
                                                ,'state':'fetched'
                                                }
                                               ,function(err,state){
                                                    if(err) return cb(err)
                                                    return cb(null)
                                                })
                                    return null
                                });
                                return null
                            }
                           ,function(e,r){
                                get_docs(test_db,function(e,docs){
                                    if(e) return cb(e)
                                    docs.should.have.property('length',99)
                                    _.each(docs
                                          ,function(resp){
                                               resp.should.have.property('key')
                                               resp.should.have.property('doc')
                                               resp.doc.should.have.property('_id')
                                           });
                                    get_docs(test_db,function(e,docs){
                                        if(e) return cb(e)
                                        should.not.exist(docs)
                                        return done()
                                    })
                                    return null
                                })
                                return null
                            })
           return null

       })

})
