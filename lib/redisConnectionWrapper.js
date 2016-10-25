'use strict';
var LOGGING_PREFIX = "RedisConnectionWrapperLib";

module.exports = function Client(base) {
    var client = this,
        getConnection,
        releaseConnection;

    function callCommand(command, args, next) {
        var connection = getConnection(),
            myTimer;
        function callback(err, data) {
            clearTimeout(myTimer);
            connection.lastCommand = command + " " + args.filter(function (arg) {
                return typeof arg !== 'function';
            }).join(' ');
            releaseConnection(connection);
            if (typeof next === "function") {//some functions don't require a callback
                next(err, data);
            }
            connection = null;
        }
        if (typeof args === "function") {//if there are no arguments for this command.
            next = args;
            args = [];
        }
        if (connection) {
            args.push(callback);//add out own callback so we can release the connection before calling next
            /*
                 From https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Function/apply
                 The apply() method calls a function with a given this value and arguments provided as an array
             */
            myTimer = setTimeout(function () {
                base.logger.error(LOGGING_PREFIX, "redis timed out. Current: '" +
                    command + " " + args.filter(function (arg) {
                        return typeof arg !== 'function';
                    }).join(' ') +
                    "'. Last command: '" +
                    connection.lastCommand +
                    "'", args);
            }, 2000);
            connection[command].apply(connection, args);
        } else {//if there are no working connections
            process.nextTick(function () {
                next("NO_CONNECTION");
            });
        }
    }

    client.hgetall = function hgetall(key, next) {
        callCommand("hgetall", [key], next);
    };

    client.hget = function hget(key, field, next) {
        callCommand("hget", [key, field], next);
    };

    client.hset = function hset(key, field, value, next) {
        callCommand("hset", [key, field, value], next);
    };

    client.set = function set(key, value, next) {
        callCommand("set", [key, value], next);
    };

    client.get = function get(key, next) {
        callCommand("get", [key], next);
    };

    client.hmget = function hmget(key, values, next) {
        callCommand("hmget", [key, values], next);
    };

    client.mget = function mget(keys, next) {
        callCommand("mget", [keys], next);
    };

    client.hmset = function hmset(key, values, next) {
        callCommand("hmset", [key, values], next);
    };

    client.hdel = function hdel(key, value, next) {
        callCommand("hdel", [key, value], next);
    };

    client.del = function del(key, next) {
        callCommand("del", [key], next);
    };

    client.sadd = function sadd(key, values, next) {
        callCommand("sadd", [key, values], next);
    };

    client.srem = function srem(key, values, next) {
        callCommand("srem", [key, values], next);
    };

    client.scard = function scard(key, next) {
        callCommand("scard", [key], next);
    };

    client.sscan = function sscan(key, index, next) {
        callCommand("sscan", [key, index], next);
    };

    client.smembers = function smembers(key, next) {
        callCommand("smembers", [key], next);
    };

    client.setex = function setex(key, seconds, value, next) {
        callCommand("setex", [key, seconds, value], next);
    };

    client.zadd = function zadd(key, score, member, next) {
        callCommand("zadd", [key, score, member], next);
    };

    client.zremrangebyrank = function zremrangebyrank(key, start, stop, next) {
        callCommand("zremrangebyrank", [key, start, stop], next);
    };

    client.zrem = function zrem(key, values, next) {
        callCommand("zrem", [key, values], next);
    };

    client.zrangebyscore = function zrangebyscore(key, min, max, next) {
        callCommand("zrangebyscore", [key, min, max], next);
    };

    client.hincrby = function hincrby(key, value, inc, next) {
        callCommand("hincrby", [key, value, inc], next);
    };

    client.expire = function expire(key, seconds, next) {
        callCommand("expire", [key, seconds], next);
    };

    client.rpush = function rpush(key, value, next) {
        callCommand("rpush", [key, value], next);
    };

    client.rpop = function rpop(key, next) {
        callCommand("rpop", [key], next);
    };

    client.lpush = function lpush(key, value, next) {
        callCommand("lpush", [key, value], next);
    };

    client.lpop = function lpop(key, next) {
        callCommand("lpop", [key], next);
    };

    client.evalsha = function evalsha() {
        // The exact amount of arguments for evalsha can vary
        var connection = getConnection(),
            args = Array.prototype.slice.call(arguments), // http://stackoverflow.com/questions/960866/how-can-i-convert-the-arguments-object-to-an-array-in-javascript
            next = args.pop(); // Take the last argument as the callback

        function callback(err, data) {
            releaseConnection(connection);
            connection = null;
            next(err, data);
        }

        if (typeof next === "function") {
            connection.evalsha(args, callback);
        } else {
            base.logger.error(LOGGING_PREFIX, "evalsha has wrong number of args", { original: arguments, args: args });
        }
    };

    client.lrange = function lrange(key, start, stop, next) {
        callCommand("lrange", [key, start, stop], next);
    };

    client.pexpire = function pexpire(key, milliseconds, next) {
        callCommand("pexpire", [key, milliseconds], next);
    };

    client.pttl = function pttl(key, next) {
        callCommand("pttl", [key], next);
    };

    client.incr = function incr(key, next) {
        callCommand("incr", [key], next);
    };

    client.send_command = function send_command(command, args, next) {
        callCommand("send_command", [command, args], next);
    };

    client.info = function info(next) {
        callCommand("info", next);
    };

    client.ping = function ping(next) {
        callCommand("ping", next);
    };

    client.getConnectionCount = base.getConnectionCount;

    getConnection = base.getConnection;

    releaseConnection = base.releaseConnection;

    client.getOrphanConnection = base.getOrphanConnection;
};
