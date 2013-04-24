var geom_utils=require('geom_utils')
var pad = geom_utils.pad

function match_district (did){
    if(/wim/.test(did)){
        // WIM data is in the wim district!
        return 'wim';
    }
    var district_regex = /^(\d{1,2})\d{5}$/;
    var match = district_regex.exec(did);
    if (match && match[1] !== undefined){
        return ['d',pad(+match[1])].join('');
    }
    // need an hpms check here
    //todo:  hpms pattern check

    // oh, hey, maybe it isn't a detector, but just the district
    var d_regex = /^d?(\d{1,2})$/i;
    match = d_regex.exec(did)
    if(match && match[1] !== undefined){
        return ['d',pad(+match[1])].join('')
    }

    return null;
}

function doc_get_dy(doc,next){
    // check the first record in the doc, determine the db, and stick it in the correct stash
    var row = doc.data[0]
    var year = row.ts.substring(0,3)
    var district
    if(row.vdsid){
        district = match_district(row.vdsid)
    }else{
        district = 'wim'
    }
    next(null,{district:district,year:year})
}
