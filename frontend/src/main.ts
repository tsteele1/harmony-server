import * as cli from "./client"
import type { MessageHandler, Message } from "@harmony/messaging"

let client: cli.Client;

const messageHandler: MessageHandler = {
    HandleMessage(message: Message): void {
        switch (message.type) {
            case "text":
                const messageContent: Record<string, any> = message.content as Record<string, any>;
                console.log(`Message from ${messageContent["sender"]}: ${messageContent["content"]}`);
                break;

            default:
                console.log(`Received Message with Unknown Type: ${message.type}`);
                break;
        }
    }
};

const connectionHandler: cli.ClientConnectionHandler = {
    HandleOpen(client: cli.Client) {
        console.log("Client Open Triggered");
    },

    HandleClose(client: cli.Client) {
        console.log("Client Close Triggered");
    }
};

let notNull = function(elem: HTMLElement | null): elem is HTMLElement {
    return elem !== null;
}

const main = () => {
    // HTML setup
    const textInput: HTMLInputElement | null = document.querySelector<HTMLInputElement>("#name-input");
    const roomCodeInput: HTMLInputElement | null = document.querySelector<HTMLInputElement>("#room-code-input");
    const connectButton: HTMLButtonElement | null = document.querySelector<HTMLButtonElement>("#connect-button");
    const disconnectButton: HTMLButtonElement | null = document.querySelector<HTMLButtonElement>("#disconnect-button");
    const messageInput: HTMLInputElement | null = document.querySelector<HTMLInputElement>("#message-input");
    const messageButton: HTMLButtonElement | null = document.querySelector<HTMLButtonElement>("#submit-message-button");
    const testReconnectButton: HTMLButtonElement | null = document.querySelector<HTMLButtonElement>("#test-reconnect-button");
    const forceDCButton: HTMLButtonElement | null = document.querySelector<HTMLButtonElement>("#force-disconnect-button");

    if (!(notNull(textInput) && notNull(roomCodeInput) && notNull(connectButton) && notNull(disconnectButton) &&
        notNull(messageInput) && notNull(messageButton) && notNull(testReconnectButton) && notNull(forceDCButton))) {
        throw TypeError("Required HTML Elements Expected to be Not Null");
    }

    connectButton.addEventListener('click', () => {
        const id: string = textInput.value;
        const roomId: string = roomCodeInput.value;

        const address: string = `ws://localhost:5051/api/rooms/${roomId}/add-player?id=${id}`;
        const wsProvider: cli.WsProvider = (address: string) => { return new window.WebSocket(address); };
        client = new cli.Client(address, wsProvider, messageHandler, connectionHandler);
    });

    disconnectButton.addEventListener('click', () => {
        client.disconnect();
    });

    messageButton.addEventListener('click', () => {
        const id: string = textInput.value;
        const message: string = messageInput.value;
        const content: Record<string, any> = {
            "sender": id,
            "content": message
        };

        client.sendMessageData("text", [], content);
    });

    testReconnectButton.addEventListener('click', () => {
        console.log("Testing Reconnection");
        client.disconnectWithDetails(3000, "Testing Reconnection");
    });

    forceDCButton.addEventListener('click', () => {
        client.stopReconnecting();
    });
};

window.addEventListener('load', main);
