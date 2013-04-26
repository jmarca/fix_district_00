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

var pad = require('geom_utils').pad

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
        return fn(null,JSON.parse(d))
    })
}
var path = require('path')
var wimfilepath = path.normalize(__dirname+'/files/wimdoc.json')
var vdsfilepath = path.normalize(__dirname+'/files/vdsdoc.json')
var vdsfilepath2 = path.normalize(__dirname+'/files/vdsdoc.json')

var files = [wimfilepath,vdsfilepath,vdsfilepath2]
var get_docs = require('../lib/get_docs')
var doc_process = require('../lib/doc_processor')
var saver = doc_process.save_stash
var processor = doc_process.doc_process

var created_locally=false
var cleardbs=[[prefix,'wim',2007].join('%2f')
             ,[prefix,'d03',2008].join('%2f')
             ,[prefix,'d11',2007].join('%2f')
             ,tracking_db
             ]

before(function(done){
    superagent.put(couch+'/'+tracking_db)
    .type('json')
    .auth(cuser,cpass)
    .end(function(r,e){
        if(test_db !== undefined) return done()

        // create a test db, the put data into it
        test_db ='test%2fgetter%2f'+Math.floor(Math.random() * 100)
        cleardbs.push(test_db)
        created_locally=true
        var bulks = make_saver(test_db)
        // generate some random docs
        var docs=[]
        for(var i=1;i<1000;i++){
            var vdsid = ['113'
                        ,pad(Math.floor(Math.random()*100))
                        ,pad(Math.floor(Math.random()*100))].join('')
            var ts='2007-02-01 23:23:00'
            docs.push({'_id':i+'_doc'
                      ,'data':[{vds_id:vdsid
                               ,ts:ts}
                              ,{vds_id:vdsid
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
                       //if(e) return done(e)
                       return cb()
                   })
                   return null
               }
              ,done);
    return null
})


describe('process real docs',function(){

    it('should save docs to wim and d03 couchdbs'
      ,function(done){
           async.eachSeries(files,function(f,cb){
               async.waterfall([function(cbw){
                                    return lame(f,cbw)
                                }
                               ,processor
                               ],cb)
               return null
           }
                           ,function(e){
                                should.not.exist(e)
                                saver(function(e){
                                    // check if couchdb save worked properly
                                    async.eachSeries(files,function(f,cb){
                                        lame(f,function(e,fdata){
                                            // get from couchdb
                                            var db = [prefix,'wim',2007].join('%2f')
                                            if(f===vdsfilepath) db = [prefix,'d03',2008].join('%2f')
                                            // get the target doc
                                            var id = fdata._id
                                            superagent.get(couch+'/'+db+'/'+id)
                                            .type('json')
                                            .set('accept','application/json')
                                            .end(function(e,r){
                                                if(e) return done(e)
                                                var saved = r.body
                                                // the revisions are never the same
                                                delete saved._rev
                                                delete fdata._rev
                                                saved.should.eql(fdata)
                                                return cb()
                                            })
                                        })
                                        return null
                                    },done)
                                    return null
                                })
                                return null
                            })
       })

})

describe('process fake docs',function(){

    it('should save docs to wim and d03 couchdbs'
      ,function(done){
           async.waterfall([function(cb){
                                get_docs(test_db,function(e,docs){
                                    if(! docs.length ) throw new Error('die')
                                    cb(null,docs)
                                })
                                return null
                            }
                           ,function(docs,cb){
                                async.each(docs
                                          ,processor
                                          ,function(e,r){
                                               should.not.exist(e)
                                               return cb(null,docs)
                                           });
                                return null
                            }
                           ,function(docs,cb){
                                saver(function(e){
                                    should.not.exist(e)
                                    return cb(null,docs)
                                })
                            }]
                          ,function(e,docs){
                               should.not.exist(e)
                               var db = [prefix,'d11',2007].join('%2f')
                               async.eachLimit(docs,2
                                               ,function(doc,cb){
                                                    var id = doc._id
                                                    if(!id)throw new Error('die')
                                                    superagent.get(couch+'/'+db+'/'+id)
                                                    .type('json')
                                                    .set('accept','application/json')
                                                    .end(function(e,r){
                                                        should.not.exist(e)
                                                        var saved = r.body
                                                        // the revisions are never the same
                                                        delete saved._rev
                                                        delete doc._rev
                                                        saved.should.eql(doc)
                                                        return cb()
                                                    })

                                                }
                                               ,done)
                           });
           return null
       })

})
