using System.Net.WebSockets;
using System.Buffers;
using System.Collections.Concurrent;

namespace Harmony {

/*
 * The Orchestrator for 1 Harmony "Game" Client and any number of Harmony
 * "Player" Clients.
 *
 * "Game": A single host client to connect to.
 * "Player": Any number of non-host clients connecting.
*/
public class Room: IServerMessaging {
    public struct Client {
        public WebSocket socket;               // Our Room's connection to the Client.
        public CancellationToken cancelToken;  // Expected: HttpContext.RequestAborted for a given Client.
        public string name;                    // Easy access to the "receiver" string for messaging.
        public bool closeCalled = false;       // Used for cutting down on redundant removal / close calls.

        public Client(string name, WebSocket socket, CancellationToken cancelToken) {
            this.name = name;
            this.socket = socket;
        }
    }

    /* Internal Variables */
    public string Id { get; }

    // An editable limit on how large of messages the Room can receive / send.
    public int bufferSize;

    private Server.RoomUpdateSignal updateNotifier;

    // For halting a updateNotifier.EmitRemove() signal.
    private CancellationTokenSource roomDeletionCancel = new CancellationTokenSource();

    // An accessor for an instance of updateNotifier.EmitRemove().
    private Task? deleteRoomTimerTask;

    // An optional lock variable for preventing new clients from connecting to the room.
    private bool locked = false;

    // Allows some Players to reconnect when the room is locked.
    private HashSet<string> allowedReconnectIds = new HashSet<string>();

    /* Messaging Variables */
    private Messenger messenger = new Messenger();

    public Messenger Messager { get { return messenger; } }

    private IServerMessageHandler messageHandler;

    public IServerMessageHandler MessageHandler { get { return messageHandler; } set { messageHandler = value; } }

    /* Internal Client Handling */
    public Client game;

    public int clientCount;

    public int maxClientCount;

    // The ConcurrentDictionary is for thread safe Gets without needing the SemaphoreSlim.
    // However, we also use a SemaphoreSlim for updating the whole of the system when adding / removing clients
    public ConcurrentDictionary<string, Client> clientMap = new ConcurrentDictionary<string, Client>();

    public string[] Receivers { get { return clientMap.Keys.ToArray(); } }

    // Open a SemaphoreSlim that only 1 thread can access at a time.
    private SemaphoreSlim clientUpdateSemaphore = new SemaphoreSlim(1, 1);

    public Room(string id, IServerMessageHandler messageHandler, Server.RoomUpdateSignal updateNotifier, int maxClientCount, int bufferSize = 4096) {
        if (maxClientCount <= 0) {
            throw new ArgumentException("Max Client Count Expected to be >= 1", "maxClientCount");
        }
        else if (bufferSize <= 0) {
            throw new ArgumentException("Buffer Size Expected to be >= 1", "bufferSize");
        }

        this.Id = id;
        this.messageHandler = messageHandler;
        this.maxClientCount = maxClientCount;
        this.bufferSize = bufferSize;
        this.updateNotifier = updateNotifier;
    }

    /*
     * Lock the room and prevent any new Players from connecting to it.
     *
     * NOTE: If a player reconnects to the Room, and they were connected
     * to the Room when this function was called, they will be allowed
     * to reconnect.
    */
    public async Task LockReceivers() {
        await clientUpdateSemaphore.WaitAsync();

        locked = true;
        Console.WriteLine($"Room {Id} LOCKED");

        // We only want people who are currently connected to the game to be able to reconnect.
        allowedReconnectIds = new HashSet<string>(Receivers);

        clientUpdateSemaphore.Release();
    }

    /*
     * Allow new players to connect to the Room.
    */
    public async Task UnlockReceivers() {
        await clientUpdateSemaphore.WaitAsync();
        locked = false;
        Console.WriteLine($"Room {Id} UNLOCKED");

        // We don't need to update the allowed reconnect set until we re-lock the room.
        clientUpdateSemaphore.Release();
    }

    /*
     * Add a Game to the Room.
     *
     * Parameters:
     *      gameName (string): NOT CURRENTLY USED. SIMPLY FOR COMPLYING WITH THE CLIENT STRUCT CONSTRUCTOR.
     *      socket (WebSocket): The WebSocket connection to the Game.
     *      ct (CancellationToken): A token for cancelling our connection to the Game (i.e. HttpContext.RequestAborted).
     *
     * Returns:
     *      true if the Game was added, else false.
    */
    public async Task<bool> AddGameAsync(string gameName, WebSocket socket, CancellationToken ct) {
        await clientUpdateSemaphore.WaitAsync();

        if (game.socket != null && game.socket.State == WebSocketState.Open) {
            clientUpdateSemaphore.Release();
            return false;
        }

        game = new Client(gameName, socket, ct);

        if (RoomDeletionQueued()) {
            ResetRoomDeletion();
        }

        clientUpdateSemaphore.Release();
        return true;
    }

    /*
     * Reset the CancellationToken for Room Deletion.
     *
     * Allows the room to try to delete itself again at a later time.
    */
    public void ResetRoomDeletion() {
        CloseRoomDeletion();
        roomDeletionCancel = new CancellationTokenSource();
    }

    /*
     * Stop any attempt at deleting the Room asynchronously and
     * free resources internally.
     *
     * NOTE: Meant to be used if you plan on closing the Room permanently.
    */
    public void CloseRoomDeletion() {
        roomDeletionCancel.Cancel();
        roomDeletionCancel.Dispose();
    }

    /*
     * Checks if the Room has signalled for its own deletion via updateNotifier.
     *
     * Returns:
     *      true if updateNotifier.EmitRemove() has been assigned, else false.
    */
    private bool RoomDeletionQueued() {
        if (deleteRoomTimerTask == null) return false;

        bool taskInProgress = !deleteRoomTimerTask.IsCompleted && !(deleteRoomTimerTask.Status == TaskStatus.Canceled) &&
                              !(deleteRoomTimerTask.Status == TaskStatus.Faulted);

        return taskInProgress;
    }

    /*
     * Close the Game's connection to the room and free its resources.
     *
     * Parameters:
     *      closeStatus (WebSocketCloseStatus): The status to send when the WebSocket closes.
     *      statusDescription (string?): An optional statusDescription to accompany the closeStatus.
     *
     * Returns:
     *      true if the Game is removed or already has been removed.
     *
     * NOTE: 
     * 1. game is now in an unusable state until another Game reconnects.
     * 2. If Room Deletion has not been queued before, it will be via RoomUpdateSignal.EmitRemove().
    */
    public async Task<bool> RemoveGameAsync(WebSocketCloseStatus closeStatus, string? statusDescription) {
        await clientUpdateSemaphore.WaitAsync();

        if (game.closeCalled) {
            Console.WriteLine("Game Has Already Been Closed");
            clientUpdateSemaphore.Release();
            return true;
        }

        if (!RoomDeletionQueued()) {
            deleteRoomTimerTask = updateNotifier.EmitRemove(Id, roomDeletionCancel.Token);
        }

        Console.WriteLine("Removing Game");

        game.closeCalled = true;
        game.socket.Dispose();
        Console.WriteLine("Game Socket Disposed");

        clientUpdateSemaphore.Release();

        Console.WriteLine(game.socket.State);
        switch (game.socket.State) {
            case WebSocketState.Connecting:
                // We're not in a valid state to be 
                Console.WriteLine("Aborting");
                game.socket.Abort();
                break;

            case WebSocketState.Open:
                Console.WriteLine("Closing");
                await game.socket.CloseAsync(closeStatus, statusDescription, CancellationToken.None);
                break;

            case WebSocketState.CloseReceived:
                Console.WriteLine("Completing Closure");
                await game.socket.CloseAsync(closeStatus, statusDescription, CancellationToken.None);
                break;

            default:
                break;
        }

        Console.WriteLine();
        return true;
    }

    /*
     * A thread safe check for if a Client can be added to a locked Room (see LockReceivers()).
     *
     * Returns:
     *      true if the room is not locked or a client is allowed to reconnect while locked, else false.
    */
    public async Task<bool> ClientCanBeAdded(string clientName) {
        await clientUpdateSemaphore.WaitAsync();
        bool result = true;

        if (locked && !allowedReconnectIds.Contains(clientName)) {
            result = false;
        }

        clientUpdateSemaphore.Release();
        return result;
    }


    /*
     * Add a Player to the Room (only differences from AddGameAsync will be listed).
     *
     * Parameters:
     *      clientName (string): The id checked when attempting to send Messages to Clients.
     *      See AddGameAsync() for other parameter definitions.
     *
     * NOTE:
     *      Throws InvalidOperationException if the Player cannot be added to the room
     *      due to exceeding max client count, or if connecting via a client name already in use.
    */
    public async Task<bool> AddClientAsync(string clientName, WebSocket socket, CancellationToken ct = default) {
        bool success = false;

        // We're going to be performing a number of thread safe operations going forward
        await clientUpdateSemaphore.WaitAsync();

        if (clientCount + 1 > maxClientCount) {
            clientUpdateSemaphore.Release();
            throw new InvalidOperationException($"Room {Id}: FULL");
        }

        clientCount++;

        if (!clientMap.ContainsKey(clientName)) {
            Client newClient = new Client(clientName, socket, ct);
            success = clientMap.TryAdd(clientName, newClient);
            clientUpdateSemaphore.Release();
            return success;
        }

        // The client already exists, so we need to be careful now
        Client client = clientMap[clientName];
        if (client.socket.State == WebSocketState.Open) {
            clientUpdateSemaphore.Release();
            throw new InvalidOperationException($"Room {Id}: Attempt to Add Already Connected Client");
        }

        Client updatedClient = client;
        updatedClient.socket = socket;
        updatedClient.cancelToken = ct;
        updatedClient.closeCalled = false;

        // client should always be the value retrieved (semaphore insurance), so updatedClient will always be updated here.
        success = clientMap.TryUpdate(clientName, updatedClient, client);

        if (!success) {
            clientCount--;
        }

        clientUpdateSemaphore.Release();
        return success;
    }

    /*
     * See RemoveGameAsync().
     *
     * NOTE:
     *      Unlike RemoveGameAsync(), RemoveClientAsync() completely removes any reference to a Player from the room
     *      (except if their name is in the list of acceptable reconnection ids when the room is locked). This is
     *      mainly so resources can be freed up rather than acknowledging that the client might never reconnect again.
     *
     *      This function also throws an InvalidOperationException in the event a Client cannot be removed and still
     *      takes up data in the Room.
    */
    public async Task<bool> RemoveClientAsync(string clientName, WebSocketCloseStatus closeStatus, string? statusDescription) {
        if (!clientMap.ContainsKey(clientName)) {
            // The client not being there is just as much of a success as a removal.
            return true;
        }

        await clientUpdateSemaphore.WaitAsync();

        Console.WriteLine($"Removing Client {clientName}");
        bool success = clientMap.TryRemove(clientName, out Client client);

        if (success && client.closeCalled) {
            Console.WriteLine($"Room {Id}: Client {clientName} was Already Deleted Somehow?");
            return true;
        }
        else if (!success && clientMap.TryGetValue(clientName, out _)) {
            return false;
        }
        else if (!success) {
            // The client was already removed, so what we're doing is effectively done
            return true;
        }

        client.closeCalled = true;
        client.socket.Dispose();
        Console.WriteLine($"Room {Id}: {clientName} Socket Disposed");

        clientCount = Math.Max(clientCount - 1, 0);

        clientUpdateSemaphore.Release();

        Console.WriteLine(client.socket.State);
        switch (client.socket.State) {
            case WebSocketState.Connecting:
                // Invalid State to Close, so we Abort instead
                Console.WriteLine("Aborting");
                client.socket.Abort();
                break;

            case WebSocketState.Open:
                Console.WriteLine("Closing Normally");
                await client.socket.CloseAsync(closeStatus, statusDescription, CancellationToken.None);
                break;

            case WebSocketState.CloseReceived:
                Console.WriteLine("Closing With Handshake Received");
                // Change this to something else to test reconnection logic on client side
                await client.socket.CloseOutputAsync(closeStatus, statusDescription, CancellationToken.None);
                break;

            default:
                // Closed, None, CloseSent, and Aborted all don't require any further action
                break;
        }

        Console.WriteLine();
        return success;
    }

    /*
     * The primary WebSocket receive loop for all client types connected to the Room.
     *
     * Handles incoming messages using the provided MessageHandler class, and
     * routes any relevant information to where it needs to go (Players -> Game, Game -> Server | some number of Players).
     *
     * Handles closing socket connections when it is finished as well.
     *
     * Parameters:
     *      clientId (string): The client we are receiving messages from.
     *      gameNotPlayer (bool): true if this client is a Game, else false.
     *
     * NOTE:
     *      Throws an InvalidDataException if the client does not have a valid WebSocket.
     *
     *      This function allocates memory equal to the buffer size provided by a developer upon
     *      class construction (default 4096 bytes). It is guaranteed to deallocate
     *      the memory upon completion of the function (errors are handled).
     *
    */
    public async Task SocketLoop(string clientId = "GAME", bool gameNotPlayer = false) {
        Client client;

        if (gameNotPlayer) {
            client = game;

            if (client.socket == null) {
                throw new InvalidDataException($"Room {Id}: Unable to Perform Socket Loop for Game");
            }
        }
        else {
            bool gotClient = clientMap.TryGetValue(clientId, out client);

            if (!gotClient || client.socket == null) {
                throw new InvalidDataException($"Room {Id}: Unable to Perform Socket Loop for Client {clientId}");
            }
        }

        byte[] messageBuffer = ArrayPool<byte>.Shared.Rent(bufferSize);
        WebSocketReceiveResult result;
        int messageByteCount = 0;
        WebSocketCloseStatus status = WebSocketCloseStatus.NormalClosure;
        string statusDescription = "Successful Closure";
        bool removed = false;

        while (client.socket.State == WebSocketState.Open) {

            try {
                result = await client.socket.ReceiveAsync(messageBuffer, client.cancelToken);
                messageByteCount = result.Count;

                if (result.MessageType == WebSocketMessageType.Close) {
                    if (result.CloseStatus != WebSocketCloseStatus.NormalClosure) {
                        status = WebSocketCloseStatus.ProtocolError;
                        statusDescription = "An Error Has Closed the Connection Client Side";
                    }

                    if (!gameNotPlayer) 
                        removed = await RemoveClientAsync(client.name, status, statusDescription);
                    else
                        removed = await RemoveGameAsync(status, statusDescription);

                    break;
                }
                else if (result.MessageType == WebSocketMessageType.Text) {
                    status = WebSocketCloseStatus.InvalidMessageType;
                    statusDescription = "Received Text and Not Binary";
                    break;
                }
            }
            catch (WebSocketException wse) {
                status = WebSocketCloseStatus.ProtocolError;
                statusDescription = wse.Message;
                Console.WriteLine($"Room {Id}: Connection to {client.name} Closed With Error Code {wse.WebSocketErrorCode}");
                break;
            }
            catch (OperationCanceledException) {
                status = WebSocketCloseStatus.EndpointUnavailable;
                statusDescription = "Going Away Now";
                break;
            }

            // For compatibility with some msgpack modules (including the one Harmony uses with TypeScript)
            // where you cannot send excess bits/bytes across the socket without getting an error when decoding.
            ArraySegment<byte> usedBuffer = new ArraySegment<byte>(messageBuffer, 0, messageByteCount);

            Message message = messenger.DecodeBinaryToMessage(usedBuffer);
            await messageHandler.HandleMessage(message, this);

            if (!gameNotPlayer && game.socket.State == WebSocketState.Open) {
                await game.socket.SendAsync(usedBuffer, WebSocketMessageType.Binary, true, game.cancelToken);
                continue;
            }
            else if (!gameNotPlayer && game.socket.State != WebSocketState.Open) {
                Console.WriteLine($"Room {Id}: Unable to Send Message to Game");
                break;
            }

            foreach (string clientName in message.Receivers) {
                Console.WriteLine($"Sending Message to Client {clientName}");
                bool sendable = clientMap.TryGetValue(clientName, out Client sendClient);

                if (!sendable || sendClient.socket == null || sendClient.socket.State != WebSocketState.Open) {
                    // The server does not care if a client can receive messages or not. Its only goal is to send.
                    // We're not going to completely derail the program because we can't send a message.
                    Console.WriteLine($"Room {Id}: Unable to Send Data to Client {clientName}. Continuing");
                    continue;
                }

                try {
                    await sendClient.socket.SendAsync(usedBuffer, WebSocketMessageType.Binary, true, sendClient.cancelToken);
                }
                catch (Exception) {
                    // See comment above about not sending messages.
                    continue;
                }
            }

        }

        ArrayPool<byte>.Shared.Return(messageBuffer);

        if (!removed && !gameNotPlayer) {
            removed = await RemoveClientAsync(client.name, status, statusDescription);
        }
        else if (!removed && gameNotPlayer) {
            removed = await RemoveGameAsync(status, statusDescription);
        }

        Console.WriteLine($"Room {Id} {removed}: Removed Client {client.name} with info {status}, {statusDescription}");
    }

    // Deallocates any non-WebSocket resources that the room needed to function.
    public void Free() {
        clientUpdateSemaphore.Dispose();
    }

    /*
     * A secondary method for sending Messages to different Player clients. Intended for compatibility with
     * IServerMessaging for sending Messages upon handling a specific message type in a MessageHandler,
     * and is not used internally in any meaningful way.
     *
     * NOTE:
     *      Unlike SocketLoop(), this function ALLOCATES AND DEALLOCATES BYTE MEMORY EVERY TIME A MESSAGE IS SENT.
     *      It is a repeatable operation, so losing a Message here is not unrecoverable, and YOU (yes, you,
     *      the developer reading this) are the only person who will be calling this function. It is recommended
     *      to make note of the sizes of messages you are expecting to send / receive, as well as the frequency
     *      with which you call this function. Otherwise, this could get out of hand.
    */
    public async Task SendMessageAsync(Message message) {
        // NOTE: This may need to be refactored to reduce the number of memory allocations.
        // However, with the infrequency for which this method will be called, it might be okay.
        byte[] messageBuffer = messenger.SerializeMessage(message);

        foreach (string clientName in message.Receivers) {
            Console.WriteLine($"Sending Message to Client {clientName}");
            bool sendable = clientMap.TryGetValue(clientName, out Client sendClient);

            if (!sendable || sendClient.socket.State != WebSocketState.Open) {
                Console.WriteLine($"Room {Id}: Unable to Send Data to Client {clientName}. Continuing");
                continue;
            }

            try {
                await sendClient.socket.SendAsync(messageBuffer, WebSocketMessageType.Binary, true, sendClient.cancelToken);
            }
            catch (Exception) {
                // We can still try sending the message to other sockets.
                continue;
            }
        }
    }

    /*
     * A secondary method for sending Messages to a Game client. Intended for compatibility with
     * IServerMessaging for sending Messages upon handling a specific message type in a MessageHandler,
     * and is not used internally in any meaningful way.
     *
     * NOTE:
     *      Unlike SocketLoop(), this function ALLOCATES AND DEALLOCATES BYTE MEMORY EVERY TIME A MESSAGE IS SENT.
     *      It is a repeatable operation, so losing a Message here is not unrecoverable, and YOU (yes, you,
     *      the developer reading this) are the only person who will be calling this function. It is recommended
     *      to make note of the sizes of messages you are expecting to send / receive, as well as the frequency
     *      with which you call this function. Otherwise, this could get out of hand.
    */
    public async Task SendMessageToGameAsync(Message message) {
        // NOTE: This may need to be refactored to reduce the number of memory allocations.
        // However, with the infrequency for which this method will be called, it might be okay.
        byte[] messageBuffer = messenger.SerializeMessage(message);

        try {
            await game.socket.SendAsync(messageBuffer, WebSocketMessageType.Binary, true, game.cancelToken);
        }
        catch (Exception) {
            Console.WriteLine($"Room {Id}: Unable to Send Message to Game from SendMessageToGameAsync()");
        }
    }
}

}
