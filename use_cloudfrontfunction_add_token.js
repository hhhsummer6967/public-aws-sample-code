// http://DomainName/Filename?token={<timestamp>-<md5hash>} 
//服务端md5hash的计算方法为md5(magic_key + uri + ts) magic_key为自定义的key，uri为请求的uri，timestamp为签算服务器生成鉴权URL的时间,UTC0
//magic_key + uri + ts 比如：magic_key = "Set_at_your_wish" uri = "/index.html" ts = "1613136000" 则md5(magic_key + uri + ts) = md5("Set_at_your_wish/index.html/1613136000")，URI要和请求的URI一致
var crypto = require('crypto');

//Update with your own key.
var magic_key = "Set_at_your_wish";

// default validation is 12 hours, update it when necessary
var time_delta = 12 * 3600; 

//Response when token ts does not match.
var response403 = {
    statusCode: 403,
    statusDescription: 'Unauthorized',
    headers: {
        'cache-control': { 
            'value': 'max-age=1296000'
        }
    }
};
function is_valid_qs(token, ts, uri) {
    var md5 = crypto.createHash('md5');
    var token_calc = md5.update(magic_key + uri + ts).digest('hex');
    var md5key = magic_key + uri + ts;
    // token mismatch or ts expired, invalid request
    if( token_calc !== token || Date.now()/1000 > parseInt(ts) + time_delta){
        console.log("Error: token mismatch or ts expired");
        console.log("token: " + token);
        console.log("token_calc: " + token_calc);
        console.log("md5 " + md5key);
        console.log("ts: " + ts);
        console.log("now: " + Date.now()/1000);
        return false;
    }
    else
    {
        return true;
    }
};

function handler(event) {
    var request_ip = event.viewer.ip;
    var request = event.request;

    // ignore auth for prefetching request
    if (request_ip === '127.0.0.1') {
        // block previously cached prefetch urls
        if (request.querystring.token){
            return response403;
        }
        else {
            return request;
        }
    }


    // If no token, then generate HTTP 403 response.
    if(!request.querystring.token ) {
        console.log("Error: No token or ts in the querystring");
        return response403;
    }

    var ts = request.querystring.token.value.split('-')[0];
    var token = request.querystring.token.value.split('-')[1];
    
    // If no token, then generate HTTP 403 response.
    if(!token ){
        console.log("Error: No token in the querystring");
        return response403;
    }
    var uri = request.uri;
    
    // invalid token, return 403 response
    if (!is_valid_qs(token, ts, uri)){
        return response403;
    }
    //如果需要，自行处理request的querystring去掉token和ts后再返回
    return request;
}

