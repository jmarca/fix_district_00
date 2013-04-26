/**
 * This module hits the database and deletes docs.
 */

var make_saver = require('couchdb_bulkdoc_saver')

var async = require('async')
var _ = require('lodash')

// all it really does is, given a list of docs and a db, it plugs in del=true into each of the docs, then fires away at the db

function deleter(db){
    return function(docs,next){
        // passed a block of docs and a db, delete the docs from the db
        // first, reformat the docs
        //console.log(docs)
        var del_docs = _.map(docs
                            ,function(doc){
                                 return {'_id':doc._id
                                        ,'_rev':doc._rev
                                        ,'_deleted':true}
                             });
        var bulk_deleter=make_saver(db)
        bulk_deleter({docs:del_docs}
                    ,function(e,r){
                         if(e) throw new Error(e)
                         _.each(r.body,function(row){
                             if(!row.ok){
                                 console.log(r.body)
                                 throw new Error('failed to remove docs'+JSON.stringify(row))
                             }
                         })
                             return next()
                     })
        return null

    }
}
module.exports=deleter
