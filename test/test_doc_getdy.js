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
var doc_get_dy = require('../lib/doc_get_dy')

var cleardbs=[]
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

describe('wim site',function(){

    it('should make a wim db'
      ,function(done){
           // open file, pass to doc_get_dy
           async.waterfall([function(cb){
                                return cb(null,filepath)
                            }
                           ,lame
                           ,doc_get_dy]
                          ,function(e,result){
                               should.exist(result)
                               result.should.have.property('district','wim')
                               result.should.have.property('year',2007)
                               return done()
                           })
           return null

       })

})
