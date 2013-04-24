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
var get_docs = require('../lib/get_docs')

var created_locally=false
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
            var vdsid = ['1132',Math.floor(Math.random()*100)].join('')
            var ts='2007-02-01 23:23:00'
            docs.push({'_id':'doc_'+i
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
    console.log({tracking_db:tracking_db
                ,test_db:test_db})
    //return done()
    async.each([tracking_db,test_db]
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


describe('get docs',function(){

    it('should get 10 docs and create the tracking db'
      ,function(done){
           async.each([1,2]
                     ,function(i,cb){
                          get_docs(test_db,function(e,docs){
                              if(e) return cb(e)
                              _.each(docs
                                    ,function(resp){
                                         resp.should.have.property('key')
                                         resp.should.have.property('doc')
                                         resp.doc.should.have.property('_id')
                                     });
                              docs.should.have.property.length(100)
                              // check the tracking db
                              couch_check({'db':tracking_db
                                          ,'doc':test_db
                                          ,'state':'fetched'
                                          }
                                         ,function(err,state){
                                              if(err) return cb(err)
                                              state.should.eql(i*100)
                                              return cb(null)
                                          })
                              return null
                          });
                          return null
                      }
                     ,done
                     );
           return null

       })

})
