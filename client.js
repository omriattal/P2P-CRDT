import * as fs from 'fs';
import Parser from './Parser.js';
import * as net from 'net';
import { argv } from 'process';
import { randomInt } from 'crypto';

/*********************************************** CONSTANTS ***************************************************/

const MILLI_TO_SECONDS = 1000;
const SPACING_PARAMETER = 4;
const CONNECTION_DELAY = 10;
const CONNECTION_LOST_ERROR = -4077;
const SINGLE_MODE = 'SINGLE';
const BATCH_MODE = 'BATCH';
const BATCH_COUNT = 10;
const inputFile = argv[2];
const data = fs.readFileSync(inputFile, 'utf-8');
const myClient = Parser.parseClient(data);
const totalClients = myClient.others.length + 1;
const timeStampTieBreaker = myClient.self.id / (totalClients + 1);
const delimiter = '<#>'; // msg protocol: <id><#><body><#><timestamp><EOL>
const goodbye = 'GOODBYE';
const UpdateOperationData = Parser.Struct('id','operation', 'timestamp', 'result');
const OperationsBufferData = Parser.Struct('operation', 'timestamp');

/*************************************************************************************************************/

/*********************************************** VARIABLES ***************************************************/

var executedOperationsCounter = 0;
var goodbyeCounter = 0;
var myTimestamp = 1.0 + timeStampTieBreaker;
var sockets = []
var updateOperationStorage = [UpdateOperationData(myClient.self.id, 'NOP', 0, myClient.self.result)];
var mode = SINGLE_MODE;
var operationsBuffer = [];

/*************************************************************************************************************/

/*********************************************** HELPER FUNCTIONS ***************************************************/

const parseMsg = (msg) => {
    const values = msg.split(delimiter);
    return [parseInt(values[0]), values[1], Parser.parseTimestamp(values[2])];
}

const deleteCharAt = (str,index) => {
    return str.substring(0, index) + str.substring(index + 1);
}

const insertCharAt = (str ,char, index) => {
    return str.substring(0, index) + char + str.substring(index);
}

const appendChar = (str, char) => {
    return insertCharAt(str,char, str.length);
}

/******************************************************************************************************************/

/*********************************************** SOCKET FUNCTIONS ***************************************************/

const sendMsgToSocket = (body, sendTimestamp, socket, callback) => {
    socket.write(myClient.self.id + delimiter + body + delimiter + sendTimestamp + Parser.EOL, callback);
}

const socketErrorCallback = (error) => {
    if (error.errno == CONNECTION_LOST_ERROR) {
        return;
    }
    console.log(error);
    console.log('error!!!!!');
}

const serverErrorCallback = (error) => {
    console.log(error);
    console.log('error@@@@@');
}

const sendMsgToAllSockets = (msg, sendTimestamp, sockets, finalCallback=null) => {
    if (finalCallback === null) {
        sockets.forEach(socket => sendMsgToSocket(msg, sendTimestamp, socket, ()=>{}))
        return;
    }
    const sendToSocketPromises = sockets.map((socket)=> {
        new Promise((resolve) => {
            sendMsgToSocket(msg, sendTimestamp, socket, resolve);
        });
    });
    Promise.all(sendToSocketPromises)
        .then(finalCallback);
}

const deteremineGoodbye = () => {
    return BATCH_MODE ? BATCH_COUNT + 3 : 5;
} 

/******************************************************************************************************************/

/*********************************************** LOGICAL FUNCTIONS ***************************************************/

const incrementMyTimestamp = () => {
    myTimestamp++;
    return myTimestamp
}

const updateTimestampFromSocket = (receivedTimestamp) => {
    myTimestamp = Math.max(Math.floor(myTimestamp), Math.floor(receivedTimestamp));
    myTimestamp += timeStampTieBreaker;
    console.log("Client updated timestamp. New timestamp: ", myTimestamp);
}

const mergeAlgorithm = (id, operation, receivedTimestamp) => {
    let firstBiggerIndex = updateOperationStorage.findIndex((opData) => receivedTimestamp < opData.timestamp)
    console.log("firstBiggerIndex = ",firstBiggerIndex);
    if (firstBiggerIndex == -1) {
        firstBiggerIndex = updateOperationStorage.length;
    }

    let result = applyUpdateOperation(operation, updateOperationStorage[firstBiggerIndex - 1].result);
    updateOperationStorage.splice(firstBiggerIndex, 0, 
                                  UpdateOperationData(id, operation, receivedTimestamp, result));
    
    console.log("Client %d started merging, from %s timestamp, on %s",
                myClient.self.id, receivedTimestamp, result)
    
    for (let i = firstBiggerIndex + 1; i < updateOperationStorage.length; i++) {
        let prevResult = updateOperationStorage[i-1].result;
        let currOperation = updateOperationStorage[i].operation;
        let currResult = applyUpdateOperation(currOperation,prevResult);
        let currTimestamp = updateOperationStorage[i].timestamp;
        updateOperationStorage[i].result = currResult;
        console.log("Client %d updated operation:%s on timestamp %s with string %s", 
        myClient.self.id, currOperation.raw, currTimestamp, currResult);
    }

   removeOperationsIfNecessary(updateOperationStorage);

    console.log("Client %d ended merging on timestamp %s with string %s", 
                myClient.self.id, myTimestamp, 
                updateOperationStorage[updateOperationStorage.length - 1].result);

    myClient.self.result = updateOperationStorage[updateOperationStorage.length - 1].result;
}

function removeOperationsIfNecessary(operationsData) {
    let found = false;
    for (let i = operationsData.length - 1; i > 0; i--) {
        if (countUniqueIDsFrom(operationsData, i + 1) == totalClients && found === true) {
            console.log("Client %d removed operation:%s on timestamp:%s with result=%s from storage", 
            myClient.self.id, operationsData[i].operation.raw, operationsData[i].timestamp, operationsData[i].result);
            operationsData.splice(i, 1);
        } else if (countUniqueIDsFrom(operationsData, i + 1) == totalClients && found === false) {
            found = true;
        }
    }
}

function countUniqueIDsFrom(operationsData, start) {
    return new Set(operationsData.map((operationData)=> operationData.id).slice(start)).size;
}

const goodbyeHandler = () => {
    goodbyeCounter++;
    if (goodbyeCounter == totalClients)
    {
        console.log("Client %d has a string replica %s", myClient.self.id, myClient.self.result);
        console.log("Client %d is exiting", myClient.self.id);
        console.log(updateOperationStorage);
        process.exit(0);
    }
}

const updateDataHandler = (receivedID, body, receivedTimestamp) => {
    console.log('Client @id:%d received an updated operation from id:%d msg:%s, timestamp: %s', 
                myClient.self.id, receivedID, body, receivedTimestamp);
    updateTimestampFromSocket(receivedTimestamp);
    incrementMyTimestamp();
    mergeAlgorithm(receivedID, Parser.operationFromRow(body), receivedTimestamp);
}

const applyUpdateOperation = (operation, str) => { 
    var result;
    const action = operation.action;
    switch (operation.type) {
        case Parser.insertOperation:
            if (action.indexOf(' ') >= 0) {
                const [characterToInsert, i] = action.split(' ');
                const index = parseInt(i);
                result = insertCharAt(str, characterToInsert, index);
            } else {
                const characterToInsert = action;
                result = appendChar(str, characterToInsert);
            }
            break;
        case Parser.deleteOperation:
            const index = parseInt(action);
            result = deleteCharAt(str, index);
            break;            
    }
    return result;
}

/******************************************************************************************************************/

/*********************************************** SOCKET CALLBACKS ***************************************************/

const operationCallback = (operation) => {
    setTimeout(() => { 
        let timestamp = incrementMyTimestamp();
        mergeAlgorithm(myClient.self.id, operation, timestamp);
        executedOperationsCounter++;
        if (mode === SINGLE_MODE) {
            sendMsgToAllSockets(operation.raw, timestamp, sockets);
        } else if (mode === BATCH_MODE) {
            operationsBuffer.push(OperationsBufferData(operation,timestamp));
            if (executedOperationsCounter % BATCH_COUNT === 0 || executedOperationsCounter == myClient.operations.length) {
                operationsBuffer.forEach((operationBufferData) => {
                    sendMsgToAllSockets(operationBufferData.operation.raw, operationBufferData.timestamp, sockets);
                });
                operationsBuffer = []
            }
        }

        if (myClient.operations.length === executedOperationsCounter) {
            console.log('Client %d finished his local string modifications', myClient.self.id);
            sendMsgToAllSockets(goodbye, myTimestamp, sockets, () => {
                setTimeout(goodbyeHandler, (deteremineGoodbye() * 1000))
            });
        }
    }, SPACING_PARAMETER * MILLI_TO_SECONDS); 
}

const connectionCallback = (socket) => {
    sockets.push(socket);
    socket.on('data', onDataCallback);
    socket.on('error', socketErrorCallback);
} 

const onDataCallback = (buffer) => {
    var flag = false;
    buffer.toString().split(Parser.EOL).reduceRight((_, msg) => {
        const [receivedID, body, receivedTimestamp] = parseMsg(msg);
        if (body === goodbye) {
            flag = true;
        } else {
            updateDataHandler(receivedID, body, receivedTimestamp);
        }
    });
    if (flag) {
        goodbyeHandler();    
    }
}
/******************************************************************************************************************/

/************************************************** MAIN **********************************************************/

const server = net.createServer().listen(myClient.self.port, myClient.self.host);
server.on('connection',connectionCallback);
server.on('error', serverErrorCallback);

console.log("Running mode:", mode);
console.log('My initial timestamp:',myTimestamp);
console.log(myClient.operations);

myClient.others.forEach((other) => {
    if (myClient.self.id < other.id) {
        console.log('id:%d try to connect to id:%d', myClient.self.id, other.id);
        const socket = net.createConnection(other.port, other.host);
        sockets.push(socket);
        socket.on('data', onDataCallback);
        socket.on('error',socketErrorCallback);
    }
});

setTimeout(()=> {
    myClient.operations.forEach(operationCallback);
}, (randomInt(CONNECTION_DELAY + myClient.self.id) * MILLI_TO_SECONDS));

/******************************************************************************************************************/
