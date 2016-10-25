'use strict';
var Base = require('./lib/createConnections'),
    base = new Base(),
    logger = console,
    async = require('async'),
    Client = require('./lib/redisConnectionWrapper'),
    LOGGING_PREFIX = "Redis",
    exportObj = {
        client: null,
        isInitialized: false,
        isLoading: false
    };

/*
 * @Description Initialize the redis connection pool
 * @param {Object} params
 * @param {String} params.host - the hostname of the redis server you are creating the connection to
 * @param {String} params.port - the port of the redis server on which you want to connect
 * @param {String} [params.name] - the name of the database you are connecting to, for error logging
 * @param {String} [params.dbNumber] - the number of the redis db you are connecting to, defaults to 0
 * @param {String} [params.minConnections] - the minimum amount of connections at all times. Defauls to 5.
 * @param {Object} [params.settings]
 * @param {Number} [params.settings.retry_max_delay] - Try and reconnect a disconnected connection with a back off up to this amount of milliseconds. Defaults to 10000.
 * @param {Number} [params.settings.connect_timeout] - Used to detect broken connections after not getting a response from redis for this amount of milliseconds. Defaults to 20000.
 * @param {Logger} [params.logger] - A logger used for for logging errors. defaults to console. If a logger is passed in it must contain the following 3 functions:
     * @param {Func}   [params.logger.error] - takes in 3 params, Prefix, messages, object. Any of these can be falsy
     * @param {Func}   [params.logger.info] - takes in 3 params, Prefix, messages, object. Any of these can be falsy
     * @param {Func}   [params.logger.warn] - takes in 3 params, Prefix, messages, object. Any of these can be falsy
 * @param {CreateConnectionCallback} - returns err, client
 */
exportObj.init = function init(params, callback) {
    if (params.logger) {
        base.logger = params.logger;
    } else {
        base.logger = logger;
    }
    function checkIfIsLoading() {
        if (exportObj.isLoading) {
            logger.warn(LOGGING_PREFIX, "Redis is loading and will wait 500ms and check again");
            return setTimeout(checkIfIsLoading, 500);
        }
        return callback();
    }
    if (exportObj.isLoading) {
        return checkIfIsLoading();
    }
    if (exportObj.isInitialized) {
        logger.warn(LOGGING_PREFIX, "Trying to initialize again!");
        return callback();
    }
    exportObj.isLoading = true;
    exportObj.isInitialized = true;

    function redisCreateCb(err) {
        if (err) {
            logger.error(LOGGING_PREFIX, err);
            return callback(err);
        }
        exportObj.isLoading = false;
        exportObj.client = new Client(base);
        callback();
    }

    base.createPool(params, redisCreateCb);
};

module.exports = exportObj;
