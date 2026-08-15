namespace Harmony {

/*
 * An abstract Factory Pattern Class for allowing the Server to create
 * a variety of different user defined IServerMessageHandler types at runtime.
 *
 * Define strings for each type of IServerMessageHandler you'd like a client
 * to be able to create for a Room, and deal with them inside CreateHandler().
*/
public abstract class MessageHandlerFactory {
    public abstract IServerMessageHandler CreateHandler(string handlerDetails);
}

}
