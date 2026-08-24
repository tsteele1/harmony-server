using Microsoft.AspNetCore.Http;
using System.Net.WebSockets;
using System.Collections.Concurrent;

namespace Harmony {

/*
 * A Handler for the Creation, Upkeep, and Deletion of all Room classes on a Harmony
 * WebSocket Server.
*/
public class Server {
    public enum RoomCreationStatus {
        CODE_CREATION_ERROR,        // Server was unable to get a valid Room Code.
        SUCCESS,                    // Room was created.
        NO_ROOM_CODES_AVAILABLE,    // Server has no Room Codes to give or does not want to give one.
        MAX_ATTEMPTS_REACHED,       // Server attempted to create a Room Code and failed.
        INVALID_ROOM_PARAMETERS,    // Exception triggered when constructing a room.
    }

    public struct RoomCreationResult {
        public RoomCreationStatus status;
        public string roomCode; // Optional: Only used if a Room was actually created.

        public RoomCreationResult(RoomCreationStatus status, string roomCode = "") {
            this.roomCode = roomCode;
            this.status = status;
        }
    }

    public enum JoinRoomResult {
        ROOM_DNE,            // The room the client / game wants to connect to does not exist.
        ALREADY_HAS_GAME,    // The room the game wants to connect to already has a game.
        MISSING_WEBSOCKET,   // The HTTP request being made is not a WebSocket request
        MISSING_CLIENT_NAME, // A client is connecting without a valid id query parameter.
        UNABLE_TO_ADD,       // Generic result for not being able to add a client / game
        SUCCESS              // Joined the Room and a WebSocket connection is established.
    }

    /*
     * A class for notifying a Room's Server about changes it wants to make.
     *
     * Currently only features support for signalling to a Server that a Room
     * should be removed soon.
    */
    public class RoomUpdateSignal {
        public Server parentServer;

        public RoomUpdateSignal(Server server) {
            this.parentServer = server;
        }

        /*
         * Have the server wait to remove a Room, then remove it.
         *
         * Supports CancellationToken/OperationCanceledException to stop the room from
         * being removed under developer defined conditions.
        */
        public async Task EmitRemove(string roomCode, CancellationToken cancelToken) {
            Console.WriteLine($"Removing Room {roomCode} in {parentServer.RoomRemovalWaitTime}");
            try {
                await Task.Delay(parentServer.RoomRemovalWaitTime, cancelToken);

                Console.WriteLine($"Deletion of Room {roomCode} Confirmed");
                Task<bool> _ = parentServer.CloseRoomAsync(roomCode);
            }
            catch (OperationCanceledException) {
                Console.WriteLine($"Canceled Deletion of Room {roomCode}");
                return;
            }
        }
    }

    /* Room Creation Variables */
    ConcurrentDictionary<string, Harmony.Room> roomMap = new ConcurrentDictionary<string, Harmony.Room>();

    Random random = new Random();

    private int maxRooms;

    MessageHandlerFactory handlerFactory;

    public TimeSpan RoomRemovalWaitTime = TimeSpan.FromMinutes(2);

    public Server(MessageHandlerFactory handlerFactory, int maxRooms = Int32.MaxValue) {
        this.handlerFactory = handlerFactory;

        if (maxRooms <= 0) {
            throw new ArgumentException("Max Room Count Must be Positive", "maxRooms");
        }
        this.maxRooms = maxRooms;
    }

    /*
     * Attempt to create a Room Code and construct a room for it.
     *
     * Parameters:
     *     clientCount (int): The maximum number of clients the Room is willing to take.
     *     messageHandlerType (string): Used with a MessageHandlerFactory given in the constructor.
     *
     * Returns:
     *      A RoomCreationResult with a RoomCreationStatus Enum denoting whether or not the
     *      room was created, as well as a room code if RoomCreationStatus.SUCCESS is returned.
    */
    public async Task<RoomCreationResult> CreateRoomAsync(int clientCount, string messageHandlerType = "default", int bufferSize = 4096) {
        // I'm gonna stick with these numbers not being editable until I inevitably test and decide I no longer want these.
        const int charLim = 5;
        int attempts = 10000;
        char[] roomCode = new char[charLim];
        const string usableChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

        bool success = false;

        // We either mathematically can't create a room code, or a developer decided we can't.
        if (roomMap.Count() == Math.Pow((double) usableChars.Length, (double) charLim) || roomMap.Count() + 1 > maxRooms) {
            return new RoomCreationResult(RoomCreationStatus.NO_ROOM_CODES_AVAILABLE);
        }

        // Defined here to save time earlier but cut out redundant creations later.
        RoomUpdateSignal signal = new RoomUpdateSignal(this);

        while (attempts > 0) {
            for (int i = 0; i < charLim; ++i) {
                roomCode[i] = usableChars[random.Next(0, usableChars.Length)];
            }

            string? tempCode = new string(roomCode);

            // Satisfying the compiler. This should not happen.
            if (tempCode == null) {
                return new RoomCreationResult(RoomCreationStatus.CODE_CREATION_ERROR);
            }

            // Prevents concurrency issues where two instances of this method try to create a Room
            // at the exact same time with the exact same code. Also prevents recreating an existing 
            // Room.
            try {
                Room room = new Harmony.Room(tempCode, handlerFactory.CreateHandler(messageHandlerType), signal, clientCount, bufferSize);
                success = roomMap.TryAdd(tempCode, room);
            }
            catch (ArgumentException) {
                return new RoomCreationResult(RoomCreationStatus.INVALID_ROOM_PARAMETERS);
            }

            if (!success) {
                attempts--;
                continue;
            }

            return new RoomCreationResult(RoomCreationStatus.SUCCESS, tempCode);
        }

        return new RoomCreationResult(RoomCreationStatus.MAX_ATTEMPTS_REACHED);
    }

    /*
     * Add a "Game" (single host client) to a given roomId.
     *
     * Parameters:
     *      context (HttpContext): Information about the HTTP request being made when this function
     *                             was called.
     *      roomId (string): A unique Id for the Room to connect to.
    */
    public async Task<JoinRoomResult> AddGameToRoomAsync(HttpContext context, string roomId) {
        // Websocket framework expects WebSocket requests.
        if (!context.WebSockets.IsWebSocketRequest) {
            return JoinRoomResult.MISSING_WEBSOCKET;
        }

        bool success = roomMap.TryGetValue(roomId, out Room? room);

        // Semanitcally equivalent, but == null satisfies the compiler later.
        if (!success || room == null) {
            return JoinRoomResult.ROOM_DNE;
        }
        else if (room.game.socket != null && 
                (room.game.socket.State == WebSocketState.Open || room.game.socket.State == WebSocketState.Connecting)) 
        {
            // This should never happen, but it helps (somewhat) alleviate room hijacking.
            return JoinRoomResult.ALREADY_HAS_GAME;
        }

        WebSocket socket = await context.WebSockets.AcceptWebSocketAsync();

        // Use context.RequestAborted to handle disconnections or the server shutting down.
        success = await room.AddGameAsync("GAME", socket, context.RequestAborted);

        if (!success) {
            return JoinRoomResult.UNABLE_TO_ADD;
        }

        Console.WriteLine($"Connected Game to Room {roomId}");
        await room.SocketLoop("GAME", true);

        return JoinRoomResult.SUCCESS;
    }

    /*
     * Add a "Player" (participating client) with id clientName to a given roomId.
     * Similar to AddGameToRoomAsync excep
     *
     * Parameters:
     *      context (HttpContext): Information about the HTTP request being made when this function
     *                             was called.
     *      roomId (string): A unique Id for the Room to connect to.
     *      clientName (string): A unique Id for client connecting.
     *
     * NOTE:
     *     Getting clientName to this function is up to the developer using this framework.
    */
    public async Task<JoinRoomResult> AddPlayerToRoomAsync(HttpContext context, string roomId, string clientName) {
        bool success = roomMap.TryGetValue(roomId, out Harmony.Room? room);

        // These are semantically equivalent, but I do this to bypass the compiler error of using room.game later
        if (!success || room == null) {
            return JoinRoomResult.ROOM_DNE;
        }
        else if (!context.WebSockets.IsWebSocketRequest) {
            return JoinRoomResult.MISSING_WEBSOCKET;
        }
        else if (!(await room.ClientCanBeAdded(clientName))) {
            // Room was locked and clientName was not on the list of valid reconnections.
            Console.WriteLine($"Room Is Locked And {clientName} Is Not On The List");
            return JoinRoomResult.UNABLE_TO_ADD;
        }

        WebSocket socket = await context.WebSockets.AcceptWebSocketAsync();

        bool addedClient = await room.AddClientAsync(clientName, socket, context.RequestAborted);

        if (!addedClient) {
            return JoinRoomResult.UNABLE_TO_ADD;
        }

        Console.WriteLine($"Connected Client {clientName} to Room {roomId}");
        await room.SocketLoop(clientName);

        return JoinRoomResult.SUCCESS;
    }

    /*
     * Clean up and free every single room on the Server.
     *
     * If a resource / room cannot be cleaned up, an InvalidOperationException is thrown.
    */
    public async Task ShutdownAsync() {
        // Another arbitrary number I put a name to.
        const int attempts = 10;

        foreach (string roomCode in roomMap.Keys) {
            int currentAttempts = attempts;

            while (currentAttempts > 0) {
                bool success = await CloseRoomAsync(roomCode);

                if (!success) {
                    currentAttempts--;
                    continue;
                }

                break;
            }

            if (currentAttempts == 0) {
                throw new InvalidOperationException($"Unable to Close Room {roomCode}.");
            }
        }

        roomMap.Clear();
    }

    /*
     * Remove any reference to a Room from the server and attempt to
     * disconnect the Game and its Clients (if possible).
     *
     * Returns:
     *      false if the room could not be deleted (i.e. resources may still be allocated),
     *      else true
     *
     * NOTE: Throws InvalidOperationException if unable to remove a game or a client
     * from a Room after trying multiple times.
    */
    public async Task<bool> CloseRoomAsync(string roomCode) {
        Console.WriteLine($"CloseRoomAsync Called for Room {roomCode}");
        const int maxAttempts = 10;
        int attempts = maxAttempts;
        bool success = roomMap.TryRemove(roomCode, out Room? room);

        if (room == null) {
            // Ensures room is null because it isn't there to begin with
            if (!roomMap.TryGetValue(roomCode, out _)) {
                return true;
            }
            else {
                return false;
            }
        }

        if (room.game.socket != null) {
            while (attempts > 0) {
                success = await room.RemoveGameAsync(WebSocketCloseStatus.NormalClosure, "Closing the Room");

                if (!success) {
                    attempts--;
                    continue;
                }

                break;
            }

            if (attempts == 0 && !success) {
                throw new InvalidOperationException($"Room {roomCode}: Unable to Remove Game Upon Room Closure");
            }
        }

        foreach (string client in room.clientMap.Keys) {
            attempts = maxAttempts;
            while (attempts > 0) {
                success = await room.RemoveClientAsync(client, WebSocketCloseStatus.NormalClosure, "Closing the Room");

                if (!success) {
                    attempts--;
                    continue;
                }

                break;
            }

            if (attempts == 0 && !success) {
                throw new InvalidOperationException($"Room {roomCode}: Unable to Remove Client {client} Upon Room Closure");
            }
        }

        // Cuts out redundant processes from doing this again soon.
        room.CloseRoomDeletion();
        room.Dispose();

        Console.WriteLine($"Removed Room {roomCode}: {success}");
        return success;
    }

    /*
     * Checks if there is a "Game" (single host client) connected in a given Room.
     *
     * Return:
     *      true if there is a connected Game, else false.
     *
     * NOTE: This status is not currently thread safe. However, a situation
     * where a Game Client reconnects to a room it is already connected to
     * is currently impossible. Therefore, this will be noted but not
     * changed until further action is required.
    */
    public bool ActiveGameInRoom(string roomCode) {
        bool gotRoom = roomMap.TryGetValue(roomCode, out Room? room);

        if (room == null || !gotRoom) {
            throw new InvalidDataException("Room Does Not Exist");
        }

        if (room.game.socket.State == WebSocketState.Connecting || room.game.socket.State == WebSocketState.Open) {
            return true;
        }

        return false;
    }
}

}
