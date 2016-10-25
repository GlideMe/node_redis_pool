/*jslint node: true*/
'use strict';
var redis = require('redis'),
    uuid = require('node-uuid'),
    async = require('async'),
    LOGGING_PREFIX = "RedisBase",
    COMMAND_CONGESTION = 1,
    CONNECTION_IDLE_TIMEOUT_MS = 60000, //60s
    MAX_CONNECTION_NUMBER = 25,
    IS_SEND_EMAIL = {
        production: true,
        staging: true
    },
    DEFAULT_MIN_CONNECTIONS = 5;

IS_SEND_EMAIL = IS_SEND_EMAIL[process.env.NODE_ENV] || false;

module.exports = function RedisBase() {
    var base = this;
    base.connectionIndex = 0;
    base.connectionsBeingCreatedCount = 0;
    base.connectionsArray = [];
    /*
     * @Description creates a single connection
     * @param {Object} params
     * @param {String} params.host - the hostname of the redis server you are creating the connection to
     * @param {String} params.port - the port of the redis server on which you want to connect
     * @param {String} [params.name] - the name of the database you are connecting to, for error logging
     * @param {String} [params.dbNumber] - the number of the redis db you are connecting to, defaults to 0
     * @param {Object} [params.settings]
     * @param {Number} [params.settings.retry_max_delay] - Try and reconnect a disconnected connection with a back off up to this amount of milliseconds. Defaults to 10000.
     * @param {Number} [params.settings.connect_timeout] - Used to detect broken connections after not getting a response from redis for this amount of milliseconds. Defaults to 20000.
     * @param {CreateConnectionCallback} - returns err, client
     */
    base.createConnection = function createConnection(params, callback) {
        var client,
            host = params.host,
            port = params.port,
            pass = params.pass,
            name = params.name,
            settings = params.settings,
            dbNumber = params.dbNumber,
            clientOnError,
            clientOnConnect,
            clientOnErrorAfterReady,
            unexpectedError,
            clientOnEnd,
            retry_max_delay = 10000,
            connect_timeout = 20000,
            clientConnectGiveUp;

        if (settings) {
            if (typeof settings.retry_max_delay === 'number') {
                retry_max_delay = settings.retry_max_delay;
            }
            delete settings.retry_max_delay;
            if (typeof settings.connect_timeout === 'number') {
                connect_timeout = settings.connect_timeout;
            }
            delete settings.connect_timeout;
        } else {
            settings = {};
        }

        settings.retry_strategy = function retry_strategy(options) {
            var err;
            if (options.total_retry_time > connect_timeout) {
                // End reconnecting after a specific timeout and flush all commands with built in error
                err = new Error("Redis connection in broken state: connection timeout exceeded.");
                err.code = "CONNECTION_BROKEN";
                clientConnectGiveUp(err);
                return;
            }
            // reconnect after
            return Math.min(options.attempt * 100, retry_max_delay);
        };

        //This will call the connect before the connection is ready so we can prep the connection properly (auth, db etc)
        settings.no_ready_check = true;

        function logConnectionError(params) {
            base.logger.error(LOGGING_PREFIX, "error in sending a message to the team about redis crashes: ", params);
        }

        clientOnError = function clientOnError(err) {
            client.removeListener('connect', clientOnConnect);

            base.logger.error(LOGGING_PREFIX, "Error connecting to redis!: " + err);
            if (IS_SEND_EMAIL && !client.errorEmailSent) {
                client.errorEmailSent = true;//only send one email per connection
                logConnectionError({
                    subject: name + " Error connecting to redis",
                    error: err.toString(),
                    stack: new Error().stack,
                    host: host,
                    port: port,
                    name: name,
                    dbNumber: dbNumber,
                    processId: process.pid
                });
            }
        };

        clientOnEnd = function clientOnEnd() {
            client.removeListener('end', clientOnEnd);
            if (IS_SEND_EMAIL && !client.errorEmailSent) {
                base.logger.error(LOGGING_PREFIX, "The connection ended unexpectedly.");
                client.errorEmailSent = true;//only send one email per connection
                logConnectionError({
                    subject: name + " Error connecting to redis",
                    stack: new Error().stack,
                    host: host,
                    port: port,
                    name: name,
                    dbNumber: dbNumber,
                    processId: process.pid
                });
            }
        };

        clientOnConnect = function clientOnConnect() {
            client.removeListener('connect', clientOnConnect);
            client.removeListener('error', clientOnError);
            client.errorEmailSent = false;//only send one email per connection

            function setDatabaseNumberCb(err) {
                if (err) {
                    base.logger.error(LOGGING_PREFIX, "redis set database error (db = " + dbNumber + "): " + JSON.stringify(err));
                    return callback(err);
                }
                client.addListener('error', clientOnErrorAfterReady);
                client.addListener('end', clientOnEnd);
                client.inUse = 0;//how many commands are being used right now. 0 duh!
                client.lastUsed = Date.now(); // the last time this client was used, so setting the lastTime to when the connection is created. used to detect idle connections
                client.id = uuid.v4();

                callback(null, client);
            }

            function setDatabaseNumber() {
                client.send_command('select', [dbNumber], setDatabaseNumberCb);
            }

            function authCb(err) {
                if (err) {
                    base.logger.error(LOGGING_PREFIX, "redis password error", err);
                    return callback(err);
                }
                setDatabaseNumber();
            }

            if (pass) {
                client.auth(pass, authCb);
            } else {
                setDatabaseNumber();
            }
        };

        unexpectedError = function unexpectedError(err) {
            client.removeListener('end', clientOnEnd);
            base.logger.error(LOGGING_PREFIX, "Unexpected error with redis! " + err.toString());
            if (IS_SEND_EMAIL && !client.errorEmailSent) {
                client.errorEmailSent = true;//only send one email per connection
                logConnectionError({
                    subject: name + " Error connecting to redis",
                    error: err.toString(),
                    stack: new Error().stack,
                    host: host,
                    port: port,
                    name: name,
                    dbNumber: dbNumber,
                    processId: process.pid
                });
            }
        };


        clientConnectGiveUp = function clientConnectGiveUp(err) {
            client.removeListener('error', clientOnErrorAfterReady);
            client.removeListener('end', clientOnEnd);
            base.logger.error(LOGGING_PREFIX, "Giving up connecting to redis");
            if (IS_SEND_EMAIL) {
                logConnectionError({
                    subject: name + " Giving up connecting to redis",
                    error: err.toString(),
                    stack: new Error().stack,
                    code: err.code,
                    host: host,
                    port: port,
                    name: name,
                    dbNumber: dbNumber,
                    processId: process.pid
                });
            }
            client.inUse = -1;//only set client to not usable after we give up on reconnecting.
        };

        clientOnErrorAfterReady = function clientOnErrorAfterReady(err) {
            if (err.code !== 'CONNECTION_BROKEN') {
                return unexpectedError(err);
            }
            clientConnectGiveUp(err);
        };

        client = redis.createClient(port, host, settings);
        client.addListener('error', clientOnError);
        client.addListener('connect', clientOnConnect);
    };

    /*
     * @Description creates a pool of connections
     * @param {Object} params
     * @param {String} params.host - the hostname of the redis server you are creating the connection to
     * @param {String} params.port - the port of the redis server on which you want to connect
     * @param {String} [params.minConnections] - the minimum amount of connections at all times. Defauls to 5.
     * @param {String} [params.name] - the name of the database you are connecting to, for error logging
     * @param {String} [params.dbNumber] - the number of the redis db you are connecting to, defaults to 0
     * @param {Object} [params.settings]
     * @param {Number} [params.settings.retry_max_delay] - Try and reconnect a disconnected connection with a back off up to this amount of milliseconds. Defaults to 10000.
     * @param {Number} [params.settings.connect_timeout] - Used to detect broken connections after not getting a response from redis for this amount of milliseconds. Defaults to 20000.
     * @param {CreatePoolCallback} - returns err
     */
    base.createPool = function createPool(params, next) {
        base.params = params;
        var times = params.minConnections || DEFAULT_MIN_CONNECTIONS,
            calledNext = false;
        async.timesSeries(times, function (time, callback) {//series so that we don't call next twice.
            base.createConnection(params, function (err, connection) {
                if (err) {
                    base.logger.error(LOGGING_PREFIX, "problem getting connection on time " + time);
                }
                if (connection) {
                    base.connectionsArray.push(connection);
                    if (!calledNext) {//should only call this once
                        calledNext = true;
                        next();
                    }
                } else if (time === times) {//end of the line and no connections! oh no!
                    if (!calledNext) {//should only call this once
                        calledNext = true;
                        next('no connection available');
                    }
                }
                callback();
            });
        });
    };

    function createConnectionCb(err, conn) {
        base.connectionsBeingCreatedCount -= 1;
        if (err) {
            base.logger.error(LOGGING_PREFIX, err);
        } else if (conn) {//else if because we were returning both client and error
            base.connectionsArray.push(conn);
        }
    }

    base.getConnection = function getConnection(attempt) {
        var index = base.connectionIndex;
        attempt = attempt || 0;
        function returnConnectionAtIndex(index) {
            base.connectionsArray[index].lastUsed = Date.now();
            return base.connectionsArray[index];
        }
        if (base.getConnectionCount() === 0) {//there are no working connections
            base.connectionsBeingCreatedCount += 1;
            base.createConnection(base.params, createConnectionCb);//so make a new one.
            return null;//the lib will return an error saying NO_CONNECTION
        }
        if (!base.connectionsArray[index]) {//we removed the connection at the index you are trying to get
            base.connectionIndex = base.connectionIndex % base.getConnectionCount();//change the index so it is definitly not bigger than the base.connectionsArray. at most this can be the index of connections array.
            // don't increment the index because we are already changing it. it will change for the next use.
            if (attempt >= base.getConnectionCount() - 1) {//to prevent an infinit loop
                return null;//the lib will return an error saying NO_CONNECTION
            }
            return getConnection(attempt + 1);
        }
        if (base.getConnectionCount() > DEFAULT_MIN_CONNECTIONS && // we have more than the default number of connections
                (Date.now() - base.connectionsArray[index].lastUsed > CONNECTION_IDLE_TIMEOUT_MS)) { // check if the connections are being under utilized
            base.logger.info(LOGGING_PREFIX, "removing connection from pool because it is idle. connection count: " +  base.getConnectionCount());
            base.connectionsArray[index].inUse = -1; // set this client to be removed from the array so no other commands get sent to this connection
            base.connectionsArray[index].errorEmailSent = true; // so no new email will be sent when it closes
            base.connectionsArray[index].quit(); // disconnect this connection gracefully (will first finish the commands that it was using performing then it will disconnect).
            attempt = -1; // reset attempts so that it wont mistakenly return NO_CONNECTIONS
        }
        base.connectionsArray[index].inUse += 1;
        base.connectionIndex = (base.connectionIndex + 1) % base.getConnectionCount();
        if (base.connectionsArray[index].inUse <= COMMAND_CONGESTION) {//if this connection is under the threshold of commands per connections
            if (base.connectionsArray[index].inUse <= 0) { // we are setting inUse on bad connections to -1 and then we incrementing by 1 so it will be 0, which is including this command.
                                                           // may be less than zero if we are calling quit because its being set to -1 and then finishing its existing commands queue before disconnecting.
                base.connectionsArray.splice(index, 1);//remove this connection from the array
                base.connectionIndex = base.connectionIndex % (base.getConnectionCount() + 1);//change the index so it is definitly not bigger than the base.connectionsArray. don't increment the index.
                return getConnection(attempt + 1);//try the next connection
            }
            return returnConnectionAtIndex(index);
        }
        if (attempt >= base.getConnectionCount() - 1) {//greater than in case we removed a connection while you are attempting to get one.
            if (base.getConnectionCount() + base.connectionsBeingCreatedCount < MAX_CONNECTION_NUMBER) { //we went all the way around the array without finding an available connection so create another one
                base.connectionsBeingCreatedCount += 1;
                base.createConnection(base.params, createConnectionCb);
            }
            return returnConnectionAtIndex(index);
        }
        base.connectionsArray[index].inUse -= 1;//we aren't using this connection so decrement in use
        return getConnection(attempt + 1);//try the next connection
    };

    base.getOrphanConnection = function getOrphanConnection(callback) {
        base.createConnection(base.params, callback);
    };

    base.releaseConnection = function releaseConnection(conn) {
        base.connectionsArray.some(function (connection) {
            if (conn.id === connection.id) {
                connection.inUse -= 1;
                return true;
            }
            return false;
        });
    };

    base.getConnectionCount = function getConnectionCount() {
        return base.connectionsArray.length;
    };
};
