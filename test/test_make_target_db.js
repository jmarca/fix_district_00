/* global require console process describe it */

var should = require('should')

var _ = require('lodash')
var async = require('async')
var superagent = require('superagent')

var env = process.env;
var cuser = env.COUCHDB_USER ;
var cpass = env.COUCHDB_PASS ;
var chost = env.COUCHDB_HOST || 'localhost';
var cport = env.COUCHDB_PORT || 5984;

// any db with something in it
// I should probably create this db

var couch = 'http://'+chost+':'+cport


//must set the env var before loading and creating the getter
var prefix='test%2fblah%2fblah'
process.env.FIX_DB_TARGET_PREFIX =prefix
var make_target_db = require('../lib/make_target_db')

var cleardbs=[]

after(function(done){
    if(!cleardbs || cleardbs.length===0) return done()
    // bail in development
    //console.log({tracking_db:tracking_db
    //            ,test_db:test_db})
    //return done()
    async.each(cleardbs,function(cleardb,cb){
        return superagent.del(couch+'/'+cleardb)
               .type('json')
               .auth(cuser,cpass)
               .end(function(e,r){
                   if(e) return done(e)
                   return cb()
               })
    },done);
    return null
})


describe('wim site',function(){

    it('should make a wim db'
      ,function(done){
           make_target_db({'district':'wim'
                          ,'year':2007}
                           ,function(e,targetdb){
                                should.exist(targetdb)
                                cleardbs.push(targetdb)
                                targetdb.should.eql([prefix,'wim',2007].join('%2f'))
                                superagent.get(couch+'/'+targetdb)
                                .type('json')
                                .set('accept','application/json')
                                .end(function(e,r){
                                    if(e) return done(e)
                                    r.error.should.not.be.ok
                                    return done()
                                })
                                return null
                            })
           return null

       })
    it('should make a vds db'
      ,function(done){
           make_target_db({'district':'12'
                          ,'year':2007}
                           ,function(e,targetdb){
                                should.exist(targetdb)
                                cleardbs.push(targetdb)
                                targetdb.should.eql([prefix,'d12',2007].join('%2f'))
                                superagent.get(couch+'/'+targetdb)
                                .type('json')
                                .set('accept','application/json')
                                .end(function(e,r){
                                    if(e) return done(e)
                                    r.error.should.not.be.ok
                                    return done()
                                })
                                return null
                            })
           return null

       })

})
