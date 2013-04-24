var geom_utils=require('geom_utils')
var pad = geom_utils.pad
var doc_get_dy = require('./doc_get_dy')
var make_target_db = require('./make_target_db')

var make_saver = require('couchdb_bulkdoc_saver')

var async = require('async')
var stash = {}

function doc_process(doc,next){

    async.waterfall([function(cb){
                         return cb(null,doc)
                     }
                    ,doc_get_dy
                    ,make_target_db
                    ,function(target_db,cb){
                         if(stash[target_db] === undefined){
                             stash[target_db]=[doc]
                         }else{
                             stash[target_db].push(doc)
                         }
                         return cb()
                     }]
                   ,function(err){
                        if (err) throw new Error(err)
                        next(null)
                    })
    return null

}

function save_stash(){
    // save stash, reset for next round
    async.eachLimit(Object.keys(stash),2
              ,function(target_db,cb){
                   var saver=make_saver(target_db)
                   saver({docs:stash[target_db]}
                        ,function(e){
                             if(e) throw new Error(e)
                             delete stash[target_db]
                             cb()
                         })
               })
}

exports.save_stash=save_stash
exports.doc_process=doc_process
