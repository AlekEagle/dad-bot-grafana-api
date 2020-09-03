require('dotenv').config();
require('./Logger')();
const http = require('http');
const app = require('express')();
const server = http.createServer(app);
const Sequelize = require('sequelize');
const Map = require('collections/map');
const WebSocket = require('ws');
const url = require('url');
const { escape, unescape } = require('querystring');
const { cpuUsage } = require('process');
const wss = new WebSocket.Server({ clientTracking: false, noServer: true });
const port = process.argv[2] || 8080,
    ip = process.argv[3] || '0.0.0.0';
const clusters = new Map();
app.use(require('express').json());
let clusterDP = {guildCount: [], cpuUsage: [], memUsage: [], ping: []};

// This function parses the query string
function parseQueryString(qs, sep, eq) {
    let parsed = {};
    qs.split(sep || '&').forEach(p => {
        let ps = p.split(eq || '=');
        parsed[unescape(ps[0])] = ps[1] ? unescape(ps[1]) : true;
    });
    return parsed;
}

// This block of code sets up the database connection and the tables in the database.
const sequelize = new Sequelize({ username: process.env.dbusername, password: process.env.dbpassword, database: process.env.dbdatabase, host: process.env.dbhost, port: process.env.dbport, dialect: 'postgres', logging: false });
sequelize.authenticate().then(() => console.log('Connection successfully established to the database!'), err => console.error('We failed to establish a connection to the database, here\'s why: ', err));
class Logs extends Sequelize.Model { };
class Errors extends Sequelize.Model { };
class Clusters extends Sequelize.Model { };
Logs.init({
    id: {type: Sequelize.DataTypes.DATE, primaryKey: true},
    data: Sequelize.DataTypes.STRING(10485760)
}, {
    sequelize
});
Logs.sync({force: true}).then(() => console.log('Table Logs synced successfully!'), err => console.error('Failed to sync Logs table to the database, here\'s why: ', err));
Errors.init({
    id: {type: Sequelize.DataTypes.DATE, primaryKey: true},
    data: Sequelize.DataTypes.STRING(10485760)
}, {
    sequelize
});
Errors.sync({force: true}).then(() => console.log('Table Errors synced successfully!'), err => console.error('Failed to sync Errors table to the database, here\'s why: ', err));
Clusters.init({
    id: {type: Sequelize.DataTypes.DATE, primaryKey: true},
    data: Sequelize.DataTypes.JSON
}, {
    sequelize
});
Clusters.sync({force: true}).then(() => console.log('Table Clusters synced successfully!'), err => console.error('Failed to sync GuildCount table to the database, here\'s why: ', err));

// This is where we handle WebSocket connections
server.on('upgrade', (req, socket, head) => {
    if (url.parse(req.url).pathname !== '/connect') {
        console.warn("A connection to an unknown path was just rejected!");
        socket.destroy();
        return;
    }
    
    console.info('A new connection was initiated, connecting...');
    wss.handleUpgrade(req, socket, head, ws => {
        wss.emit('connection', ws, req);
    })
});

// This block of code will parse the users that come from the .env file as defined in the "Users" definition.
let users = {};
process.env.Users.split(',').forEach(u => {
    let user = u.split(':');
    users[user[0]] = user[1];
});

// Here is how we handle authentication for normal http requests
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    if (!req.headers.authorization) {
        res.header('WWW-Authenticate', 'Basic');
        res.sendStatus(401);
        return;
    }
    let auth = req.headers.authorization.split(' ')[1];
    auth = new Buffer.from(auth, 'base64').toString();
    if (Object.keys(users).indexOf(auth.split(':')[0]) !== -1 ? Object.values(users)[Object.keys(users).indexOf(auth.split(':')[0])] === auth.split(':')[1] : false) {
        res.header('Set-Cookie', 'auth=' + req.headers.authorization);
        next();
    } else {
        res.header('WWW-Authenticate', 'Basic');
        res.sendStatus(401);
    }
});


server.listen(port, ip);
server.on('listening', () => {
    console.info(`Server listening on ${server.address().address}:${server.address().port}`);
});

wss.on('connection', ws => {
    ws.isAlive = true;
    ws.on('pong', () => {
        ws.isAlive = true;
    });
    let identifyTimeout = setTimeout(() => {
        console.info("The connection didn't identify in time, disconnecting!");
        ws.close(4002);
    }, 10000);
    ws.send(JSON.stringify({op: 0, d: {heartbeatInterval: 10000}}));
    ws.on('message', d => {
        try {
            JSON.parse(d);
        }catch (err) {
            console.info("The connection did not send JSON data, disconnecting!");
            if (ws.ID !== undefined) clusters.delete(ws.ID);
            ws.close(1003);
            return;
        }
        let json = JSON.parse(d);
        switch(json.op) {
            case 2:
                // Identify payload
                if(!json.d.token || json.d.clusterID === undefined, !json.d.clusterCount) {
                    console.info("The connection sent an invalid payload, disconnecting!");
                    ws.close(4004, "Invalid Payload");
                    return;
                }
                if (json.d.clusterID >= json.d.clusterCount) {
                    console.info("The connection sent an invalid clusterID, disconnecting!");
                    ws.close(4005, "Cluster ID doesn't match with cluster count");
                    return;
                }
                if (clusters.has(json.d.clusterID)) {
                    console.info("A connection tried to identify with a clusterID that was already connected, disconnecting!");
                    ws.close(4007, "Cluster ID already connected");
                    return;
                }
                auth = new Buffer.from(json.d.token, 'base64').toString();
                if (Object.keys(users).indexOf(auth.split(':')[0]) !== -1 ? Object.values(users)[Object.keys(users).indexOf(auth.split(':')[0])] === auth.split(':')[1] : false) {
                    console.info(`Connection with username: ${auth.split(':')[0]} identified successfully as cluster: ${json.d.clusterID}`);
                    ws.ID = json.d.clusterID;
                    ws.clusterCount = json.d.clusterCount;
                    clusters.set(json.d.clusterID, ws);
                    ws.send(JSON.stringify({op: 1, d: false}))
                    clearTimeout(identifyTimeout);
                    if (!clusters.every(wwss => wwss.clusterCount === json.d.clusterCount)) {
                        console.log("New cluster count identified, disconnecting all clusters");
                        clusters.forEach(cluster => {
                            cluster.close(4006, "New cluster count");
                            clusters.delete(cluster.ID);
                        });
                    }
                    if (clusters.size === json.d.clusterCount) {
                        console.info("All clusters connected, notifying all clusters");
                        clusters.forEach(wwss => {
                            wwss.send(JSON.stringify({op: 8, d: false}));
                        });
                    }
                } else {
                    console.warn("The connection sent an invalid token, disconnecting!");
                    ws.close(4003, "Invalid Token");
                }
            break;
            case 3:
                // Update all
                if (!json.d.guildCount === undefined || !json.d.ping === undefined || !json.d.cpuUsage === undefined || !json.d.memUsage === undefined) {
                    console.info("The connection sent an invalid payload, disconnecting!");
                    if (ws.ID !== undefined) clusters.delete(ws.ID);
                    ws.close(4004), "Invalid Payload";
                    return;
                }
                clusterDP.guildCount[ws.ID] = json.d.guildCount;
                clusterDP.memUsage[ws.ID] = json.d.memUsage;
                clusterDP.cpuUsage[ws.ID] = json.d.cpuUsage;
                clusterDP.ping[ws.ID] = json.d.ping;
                ws.send(JSON.stringify({op: 4, d: false}));
                if (clusterDP.guildCount.filter(e => e !== undefined).length === ws.clusterCount) {
                    
                    Clusters.create({id: new Date().toISOString(), data: clusterDP}).then(() => {
                        clusters.forEach(wwss => {
                            wwss.send(JSON.stringify({op: 7, d: clusterDP}));
                        });
                        clusterDP = {guildCount: [], cpuUsage: [], memUsage: [], ping: []};
                    });
                }
            break;
            case 5:
                if (!json.d) {
                    console.info("The connection sent an invalid payload, disconnecting!");
                    if (ws.ID !== undefined) clusters.delete(ws.ID);
                    ws.close(4004, "Invalid Payload");
                    return;
                }
                Logs.create({id: new Date().toISOString(), data: json.d}).then(() => ws.send(JSON.stringify({op:4, d: false})));
            break;
            case 6:
                if (!json.d) {
                    console.info("The connection sent an invalid payload, disconnecting!");
                    if (ws.ID !== undefined) clusters.delete(ws.ID);
                    ws.close(4004, "Invalid Payload");
                    return;
                }
                Errors.create({id: new Date().toISOString(), data: json.d}).then(() => ws.send(JSON.stringify({op:4, d: false})));
            break;
            default:
                ws.close(4001, "Invalid opcode");
                if (ws.ID !== undefined) clusters.delete(ws.ID);
        }
    });
    ws.on('close', (code, reason) => {
        if (clusterDP[ws.ID]) delete clusterDP[ws.ID];
        if (code !== 1000) {
            console.warn(`A websocket closed with an error!`, code, reason);
        }else {
            console.info('Websocket closed cleanly!');
        }
        if (ws.ID !== undefined) clusters.delete(ws.ID);
    });
    ws.on('error', err => {
        console.warn(`A websocket errored !`, err);
        if (ws.ID !== undefined) clusters.delete(ws.ID);
    });
});

process.on('SIGINT', () => {
    console.info('Exiting gracefully...');
    clusters.forEach(ws => {
        ws.close(1012, "Server Closing");
    });
    process.exit();
});

process.on('uncaughtException', () => {
    clusters.forEach(ws => {
        ws.close(1011, "Internal server error");
    });
    process.exit(1);
});

setInterval(() => {
    clusters.forEach(ws => {
        if (!ws.isAlive) {
            ws.terminate();
            console.info(`Cluster ${ws.ID} did not respond to a ping, disconnecting.`);
            if (ws.ID !== undefined) clusters.delete(ws.ID);
        }

        ws.isAlive = false;
        ws.ping(null, false, true);
    });
}, 5000);

