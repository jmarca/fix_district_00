/* global process require console __dirname */

/**
 * the point of this program is to fix the d00 database by assigning
 * each document to the correctly named database
 */

// set options by environment variables
var env = process.env;

var async = require('async')
var _ = require('lodash')

var couchCache= require('calvad_couch_cacher').couchCache
var reducer= require('calvad_reducer')
var globals =require('./globals')
var pad = globals.pad
var couch_cache = couchCache();
var superagent = require('superagent')
var viewer = require('couchdb_get_views')

var cuser = env.COUCHDB_USER ;
var cpass = env.COUCHDB_PASS ;
var chost = env.COUCHDB_HOST || 'localhost';
var cport = env.COUCHDB_PORT || 5984;


var couch = 'http://'+chost+':'+cport+'/'


var district='d00'
var tracking = 'vdsdata%2ftracking'




// logic.  loop until all files have been fetched from this monstrous db
// store my current place in the tracking db, so that I can crash and start over again
// because jeez, 100G is a lot

// start with 100 docs at a time, ramp up from there.

var stash={}
async.waterfall([ddq_cdb.get_real_dbs
                ,function(dbsFromDistrict,cb1){
                     // for each district, year, get all detectors
                     // with data, and run the collate thing
                     var all_detectors=[]
                     async.eachLimit(dys,2,function(dy,cb2){

                         async.series([function(cb){return make_target_db(dy,cb)}
                                      ,function(cb3){
                                           // get all detectors in district
                                           console.log(dy)
                                           var dbs = dbsFromDistrict(dy.district,dy.year)
                                           dbs=_.flatten(dbs)
                                           //console.log(dbs)
                                           async.each(dbs
                                                     ,function(db,cb3){
                                                          console.log(db)
                                                          // strip off just the db, not the host
                                                          var thedb = /:598\d\/(.*)/.exec(db)

                                                          // use couch view to get all the detector ids
                                                          viewer({'db':thedb[1]
                                                                 ,'view':collect_view
                                                                 ,'group':true
                                                                 ,'group_level':1
                                                                 ,'reduce':true
                                                                 },function(err,result){
                                                                       if(result===undefined){
                                                                           return cb3()
                                                                       }
                                                                       console.log(result)
                                                                       _.each(result.rows
                                                                             ,function(row){
                                                                                  if(row.key[0]){
                                                                                      all_detectors.push(
                                                                                          {detector:row.key[0]
                                                                                          ,year:dy.year})
                                                                                  }
                                                                                  return null
                                                                              });
                                                                       return cb3()
                                                                   })
                                                      },function(e){
                                                            if(e) throw new Error(e)
                                                            return cb3(null)
                                                        });
                                           return null

                                       }]
                                      ,cb2
                         )
                         return null
                     },function(e){
                           if(e) throw new Error(e)
                           // finish up
                           // all detectors are stashed in all_detectors.  do the work
                           return cb1(null,all_detectors)
                       })
                 }
                ,function(detectors,cb1){
                     // pass each detector to the collation exercise
                     async.eachLimit(detectors,5
                                    ,function(dy,cb2){
                                         async.waterfall([
                                             function(cb){
                                                 console.log(dy)
                                                 var did=dy.detector
                                                 var year=dy.year
                                                 var ts = new Date(+year,0,1,0,0).getTime()/ 1000
                                                 var endts =  new Date(+year+1,0,0,0,0).getTime()/1000

                                                 var features = [{'properties':{'detector_id':did
                                                                               ,'ts':ts
                                                                               ,'endts':endts
                                                                               }}]
                                                 return cb(null,features)
                                             }
                                             // get freeway, etc from tracking db
                                           ,function(features,cb){
                                                // hardcode because whatever
                                                checker({'db':'vdsdata%2ftracking'
                                                        ,'doc':dy.detector
                                                        ,'year':dy.year
                                                        ,'state':'properties'}
                                                       ,function(err,state){
                                                            if(err) throw new Error (err)
                                                            if(!state){
                                                                console.log('nothing in vdsdata%2ftracking for '+JSON.stringify(dy))
                                                                features[0].properties.segment_length = 1
                                                                features[0].properties.link_length = 1
                                                                features[0].properties.len = 1
                                                                // hackity hack ugly make it work.
                                                                features[0].properties.freeway = 9999
                                                            }else{
                                                                var props = state[0]
                                                                features[0].properties.segment_length = props.segment_length
                                                                features[0].properties.link_length = props.segment_length
                                                                features[0].properties.len = props.segment_length
                                                                // hackity hack ugly make it work.
                                                                features[0].properties.freeway = props.freeway
                                                            }
                                                            cb(null,features)
                                                        })
                                                return null
                                            }
                                           ,function(features,cb){
                                                var doneGeo = dgq('collate'
                                                                 ,function(accum,docid,next){
                                                                      return function(err){
                                                                          if(err) throw new Error(err);
                                                                          console.log('done'+dy.detector)
                                                                          var featurehash = {
                                                                              'properties' : {'document': 'collate '+dy.detector
                                                                                             }}
                                                                          var stash = accum.stash_out(featurehash)
                                                                          console.log('stashed')
                                                                          return next()
                                                                      }
                                                                  }
                                                                 ,{}
                                                                 ,function(a,b,c){
                                                                      return cb()
                                                                  })
                                                return doneGeo(null,features);
                                            }]
                                                        ,cb2)
                                         return null
                                     }
                                    ,function(e){
                                         if(e) throw new Error(e)
                                         console.log('done with all detectors collation')
                                         return cb1()
                                     })
                 }]
               ,function(e){
                    // done
                    return null
                });

console.log('next tick the magick happens')
