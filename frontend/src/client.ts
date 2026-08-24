import type { MessageHandler } from "@harmony/messaging"
import * as messaging from "@harmony/messaging"

// Allows injection of different WebSocket providers into the Client
// (For most purposes, this will just return window.WebSocket(), but it's still nice).
export type WsProvider = (address: string) => any;

// An extra convenience for doing things when the connection opens / closes
// For example, you might want to be able to alter the HTML of a web page
// if the connection permanently closes, even if it's because of a failure to connect.
export interface ClientConnectionHandler {
    HandleOpen: (client: Client) => void;
    HandleClose: (client: Client) => void;
}

/*
 * A web-client for Harmony WebSocket connections.
 * Originally designed to be a "player" connecting to a server
 * with a room created by a "host" (e.g. the C# Client and C# Server).
*/
export class Client {
    /* Connection and WebSocket Handling*/
    address: string;
    wsProvider: WsProvider;
    // Typed as any for plausible scalability to other WebSocket interfaces (e.g. Node's ws module)
    ws: any;
    messageHandler: MessageHandler;
    connectionHandler: ClientConnectionHandler;

    /* Reconnect Logic */
    maxRetries: number;
    retries: number = 0;
    reconnectDelay: number = 1000;
    maxReconnectDelay: number = 30000;
    stopReconnect: boolean = false;

    /*
     * Parameters:
     *      address (string): The URL to connect to with a WebSocket.
     *
     *      wsProvider (WsProvider): A function returning the desired WebSocket format to use.
     *              NOTE: This format is expected to be compliant with the same API that window.WebSocket() uses.
     *                    If it does not, this will not work as expected.
     *
     *      messageHandler (messageHandler): A handler for what Client should do when it receives a Harmony Message.
     *
     *      connectionHandler (ClientConnectionHandler): A handler for what Client should do on connect / disconnect.
     *
     *      maxReconnectRetries (number): The number of times Client will attempt to reconnect to address AFTER it
     *                                    tries connecting once (i.e. 10 retries means 11 attempts at connection).
     *              NOTE: A maxReconnectRetires value < 0 will throw a RangeError.
     *
     * NOTE:
     *      The constructor calls Client.connect() immediately. You do not need to call it yourself.
    */
    constructor(address: string, wsProvider: WsProvider, messageHandler: MessageHandler,
        connectionHandler: ClientConnectionHandler, maxReconnectRetries: number = 10) {
        this.address = address;
        this.wsProvider = wsProvider;
        this.messageHandler = messageHandler;
        this.connectionHandler = connectionHandler;

        if (maxReconnectRetries < 0) {
            throw RangeError("maxReconnectRetries Expected to be 0 or Greater.");
        }
        this.maxRetries = maxReconnectRetries;

        this.connect();
    }

    /*
     * Reinitialize the WebSocket and connect to the pre-provided address.
     *
     * In the event a connection fails unexpectedly, reconnection will be attempted until
     * maxRetries has been reached, or a reconnection is made.
    */
    connect() {
        // WebSockets are only good once, so we reset it on each connection.
        this.ws = this.wsProvider(this.address);

        // For MessagePack compatibility
        this.ws.binaryType = "arraybuffer";

        this.ws.onopen = () => {
            console.log(`Connected: ${this.address}`);
            this.retries = 0;
            this.stopReconnect = false;
            this.connectionHandler.HandleOpen(this);
        };

        this.ws.onclose = (event: CloseEvent) => {
            console.log(`Closed: ${event.code} ${event.reason}`);
            // We could maybe include 1001 (server shutdown or user going away) as well here,
            // but the reconnections won't hurt to try.
            if (event.code == 1000) {
                this.close();
                return;
            }

            if (this.stopReconnect) {
                this.close("Manual Disconnect");
            }
            else if (this.retries >= this.maxRetries || this.stopReconnect) {
                this.close("Unable to Reconnect to Server: Max Retries Reached");
                return;
            }

            // The braces around 2 ** this.retries is just for readability. I know it doesn't do anything different.
            const baseReconnectDelay: number = Math.min(this.reconnectDelay * (2 ** this.retries), this.maxReconnectDelay);
            const randomTimingOffset = Math.random() * baseReconnectDelay * 0.5;
            const currentReconnectDelay = baseReconnectDelay + randomTimingOffset;
            setTimeout(() => { this.retries++; this.connect(); }, currentReconnectDelay);
        };

        this.ws.onmessage = (event: MessageEvent) => {
            // event.data should be of type ArrayBuffer, so a direct conversion like this is possible
            // NOTE: this does not create a copy of event.data as a Uint8Array. It creates a view.
            const message: messaging.Message = messaging.deserializeMessage(new Uint8Array(event.data));

            this.messageHandler.HandleMessage(message);
        };

        this.ws.onerror = (error: ErrorEvent) => {
            console.error("Websocket Error:", error);
        };
    }

    /* A collection of Message sending functions for developer options and convenience. */
    sendMessageBinary(message: Uint8Array) {
        this.ws.send(message);
    }

    sendMessage(message: messaging.Message) {
        this.sendMessageBinary(messaging.serializeMessage(message));
    }

    sendMessageData(type: string, receivers: string[], content: Record<string, any>) {
        this.sendMessageBinary(messaging.createAndSerializeMessage(type, receivers, content));
    }

    disconnect() {
        this.ws.close();
    }

    /*
     * Parameters:
     *      code (number): Either 1000 or a number in the range [3000, 4999] (custom errors).
     *              NOTE: None of the standard error codes are supported except for 1000. This
     *              is in compliance with window.WebSocket(), not a choice of Harmony.
     *
     *      reason (string): A description the WebSocket on the other end can read.
     *              NOTE: This is made non-optional because it is highly recommended by the
     *              underlying API anyways. You could just as easily avoid it by giving ''.
    */
    disconnectWithDetails(code: number, reason: string) {
        this.ws.close(code, reason);
    }

    stopReconnecting() {
        this.stopReconnect = true;
    }

    private close(description: string = "") {
        if (description !== "") {
            console.log(description);
        }

        this.retries = 0;
        this.stopReconnect = false;
        this.connectionHandler.HandleClose(this);
    }
}
