export default class Parser {
    static insertOperation = 'insert';
    static deleteOperation = 'delete';
    static host = '127.0.0.1'; // localhost or 127.0.0.1
    static EOL = '\r\n';
    static Struct = (...keys) => ((...v) => keys.reduce((o, k, i) => {o[k] = v[i]; return o} , {}))
    static ClientData = Parser.Struct('id', 'host', 'port', 'result');
    static Operation = Parser.Struct('type', 'action', 'raw');
    static Client = Parser.Struct('self', 'others', 'operations');

    static operationFromRow(row) {
        const separator = row.indexOf(' ');
        const type = row.slice(0, separator);
        const action = row.slice(separator + 1, row.count);

        return Parser.Operation(type, action, row);
    }

    static clientFromRowAndInput(row, initialInput) {
        const data = row.split(' ');
        const id = parseInt(data[0]);
        const host = data[1];
        const port = data[2];
        
        return Parser.ClientData(id, host, port, initialInput);
    }

    static parseClient(data) {
        const dataSections = data.split(Parser.EOL+Parser.EOL);
        const privateInformation = dataSections[0].split(Parser.EOL);
        const clientsInformation = dataSections[1].split(Parser.EOL);
        const operations = dataSections[2].split(Parser.EOL);

        const id = parseInt(privateInformation[0]);
        const port = privateInformation[1];
        const initialInput = privateInformation[2];
        
        var self = Parser.ClientData(id, Parser.host, port, initialInput);
        var clients = clientsInformation.map(row => Parser.clientFromRowAndInput(row, initialInput));
        var myOperations = operations.map(row => Parser.operationFromRow(row));
        
        return Parser.Client(self, clients, myOperations);
    }

    static parseTimestamp(timestamp) {
        return parseFloat(timestamp);
    }
}