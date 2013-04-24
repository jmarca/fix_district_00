var superagent = require('superagent')
var cuser = env.COUCHDB_USER ;
var cpass = env.COUCHDB_PASS ;
var chost = env.COUCHDB_HOST || 'localhost';
var cport = env.COUCHDB_PORT || 5984;


var couch = 'http://'+chost+':'+cport+'/'

var globals =require('./globals')
var pad = globals.pad

var target_dbs={}

/**
 * take in a district,year object, and return the target database,
 * creating it the first time I see it, accepting failure to create
 * gracefully
 */
function make_target_db(dy,cb){
    var d = dy.district
    var y = dy.year
    var targetdb = ['imputed','collated','d'+pad(d),y].join('%2f')
    if(target_dbs[targetdb]) return cb(null,targetdb)
    target_dbs[targetdb] = 1
    superagent.put(couch+targetdb)
    .type('json')
    .set('accept','application/json')
    .auth(cuser,cpass)
    .end(function(e,r){
        console.log(couch+targetdb)
        if(!r) {
            throw new Error('no response?')
        }
        if(r.body)console.log(r.body)
        if(r.error)console.log(r.error)
        return cb(null,targetdb)
    })
    return null
}
