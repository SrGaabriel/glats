import glats/internal/subscription
import glats/internal/util
import gleam/dict
import gleam/dynamic
import gleam/dynamic/decode
import gleam/erlang/atom
import gleam/erlang/process
import gleam/io
import gleam/list
import gleam/option.{None, Some}
import gleam/otp/actor
import gleam/result

pub type Connection =
  process.Subject(ConnectionMessage)

/// A single message that can be received from or sent to NATS.
///
pub type Message {
  Message(
    topic: String,
    headers: dict.Dict(String, String),
    reply_to: option.Option(String),
    body: String,
  )
}

/// Option that can be specified when publishing a message.
///
pub type PublishOption {
  /// Provide a reply topic with the message that the receiver
  /// can send data back to.
  ReplyTo(String)
  /// Headers to add to the message.
  Headers(List(#(String, String)))
}

/// Option that can be specified when subscribing to a topic.
///
pub type SubscribeOption {
  /// Subscribe to a topic as part of a queue group.
  QueueGroup(String)
}

/// Server info returned by the NATS server.
///
pub type ServerInfo {
  ServerInfo(
    server_id: String,
    server_name: String,
    version: String,
    go: String,
    host: String,
    port: Int,
    headers: Bool,
    max_payload: Int,
    proto: Int,
    jetstream: Bool,
    auth_required: option.Option(Bool),
  )
}

/// Options that can be passed to `connect`.
///
pub type ConnectionOption {
  /// Set a username and password for authentication.
  UserPass(String, String)
  /// Set a token for authentication.
  Token(String)
  /// Set an NKey seed for authentication.
  NKeySeed(String)
  /// Set a JWT for authentication.
  JWT(String)
  /// Set a CA cert for the server connection.
  CACert(String)
  /// Set client certificate key pair for authentication.
  ClientCert(String, String)
  /// Set a custom inbox prefix used by the connection.
  InboxPrefix(String)
  /// Set a connection timeout in milliseconds.
  ConnectionTimeout(Int)
  /// Enable the no responders behavior.
  EnableNoResponders
}

/// Errors that can be returned by the server.
///
pub type Error {
  NoReplyTopic
  Timeout
  NoResponders
  Unexpected
}

//            //
// Connection //
//            //

/// Message sent to a NATS connection process.
pub opaque type ConnectionMessage {
  Subscribe(
    from: process.Subject(Result(Int, Error)),
    subscriber: process.Pid,
    topic: String,
    opts: List(SubscribeOption),
  )
  Unsubscribe(from: process.Subject(Result(Nil, Error)), sid: Int)
  Publish(
    from: process.Subject(Result(Nil, Error)),
    topic: String,
    body: String,
    opts: List(PublishOption),
  )
  Request(
    from: process.Subject(fn() -> Result(Message, Error)),
    topic: String,
    body: String,
    opts: List(PublishOption),
    timeout: Int,
  )
  GetServerInfo(from: process.Subject(Result(ServerInfo, Error)))
  GetActiveSubscriptions(from: process.Subject(Result(Int, Error)))
  Exited(process.ExitMessage)
}

/// Message received by a subscribing subject.
///
pub type SubscriptionMessage {
  ReceivedMessage(
    conn: Connection,
    sid: Int,
    status: option.Option(Int),
    message: Message,
  )
}

// State kept by the connection process.
type State {
  State(nats: process.Pid, subscribers: dict.Dict(process.Pid, Int))
}

// Gnat's GenServer start_link.
@external(erlang, "Elixir.Gnat", "start_link")
fn gnat_start_link(
  settings settings: dict.Dict(atom.Atom, dynamic.Dynamic),
) -> Result(process.Pid, dynamic.Dynamic)

// Gnat's publish function.
@external(erlang, "Elixir.Gnat", "pub")
fn gnat_pub(
  a: process.Pid,
  b: String,
  c: String,
  d: List(PublishOption),
) -> atom.Atom

// Gnat's request function.
@external(erlang, "Elixir.Gnat", "request")
fn gnat_request(
  a: process.Pid,
  b: String,
  c: String,
  d: List(#(atom.Atom, dynamic.Dynamic)),
) -> Result(dynamic.Dynamic, atom.Atom)

// Gnat's subscribe function.
@external(erlang, "Elixir.Gnat", "sub")
fn gnat_sub(
  a: process.Pid,
  b: process.Pid,
  c: String,
  d: List(SubscribeOption),
) -> Result(Int, String)

// Gnat's unsubscribe function.
@external(erlang, "Elixir.Gnat", "unsub")
fn gnat_unsub(
  a: process.Pid,
  b: Int,
  c: List(#(atom.Atom, String)),
) -> atom.Atom

// Gnat's server_info function.
@external(erlang, "Elixir.Gnat", "server_info")
fn gnat_server_info(a: process.Pid) -> dynamic.Dynamic

// Gnat's active_subscriptions function.
@external(erlang, "Elixir.Gnat", "active_subscriptions")
fn gnat_active_subscriptions(a: process.Pid) -> Result(Int, dynamic.Dynamic)

// ffi server info decoder
@external(erlang, "Elixir.Glats", "decode_server_info")
fn glats_decode_server_info(a: dynamic.Dynamic) -> Result(ServerInfo, Error)

/// Starts an actor that handles a connection to NATS using the provided
/// settings.
///
/// ## Example
///
/// ```gleam
/// connect(
///   "localhost",
///   4222,
///   [
///     CACert("/tmp/nats/ca.crt"),
///     InboxPrefix("_INBOX.custom.prefix."),
///   ],
/// )
/// ```
///
pub fn connect(host: String, port: Int, opts: List(ConnectionOption)) {
  // Start actor for NATS connection handling.
  // This just starts Gnat's GenServer module linked to
  // the actor process and translates commands.
  actor.new_with_initialiser(5000, fn(my_subject) {
    process.trap_exits(True)

    // Start linked process using Gnat's start_link
    case gnat_start_link(build_settings(host, port, opts)) {
      Ok(pid) -> {
        let selector =
          process.new_selector()
          |> process.select_trapped_exits(Exited)
          |> process.select(my_subject)

        actor.initialised(State(nats: pid, subscribers: dict.new()))
        |> actor.selecting(selector)
        |> actor.returning(my_subject)
        |> Ok
      }
      Error(_) -> Error("starting connection failed")
    }
  })
  |> actor.on_message(handle_command)
  |> actor.start
}

// Runs for every command received.
//
fn handle_command(state: State, message: ConnectionMessage) {
  case message {
    Exited(em) ->
      case em.pid == state.nats {
        // Exited process is Gnat and we can't recover from this
        True -> actor.stop()
        // Exited process is something else so we check if it's
        // in the map of subscribers and unsubscribe it.
        False ->
          case dict.get(state.subscribers, em.pid) {
            Ok(sid) -> {
              gnat_unsub(state.nats, sid, [])
              actor.continue(
                State(
                  ..state,
                  subscribers: dict.delete(state.subscribers, em.pid),
                ),
              )
            }
            Error(Nil) -> {
              io.println("exited process not found in map of subscribers")
              actor.continue(state)
            }
          }
      }
    Publish(from, topic, body, opts) ->
      handle_publish(from, topic, body, opts, state)
    Request(from, topic, body, opts, timeout) ->
      handle_request(from, topic, body, opts, timeout, state)
    Subscribe(from, subscriber, topic, opts) ->
      handle_subscribe(from, subscriber, topic, opts, state)
    Unsubscribe(from, sid) -> handle_unsubscribe(from, sid, state)
    GetServerInfo(from) -> handle_server_info(from, state)
    GetActiveSubscriptions(from) -> handle_active_subscriptions(from, state)
  }
}

// Handles a single server info command.
//
fn handle_server_info(from, state: State) {
  gnat_server_info(state.nats)
  |> glats_decode_server_info
  |> result.map_error(fn(_) { Unexpected })
  |> process.send(from, _)

  actor.continue(state)
}

// Handles a single active subscriptions command.
//
fn handle_active_subscriptions(from, state: State) {
  gnat_active_subscriptions(state.nats)
  |> result.map_error(fn(_) { Unexpected })
  |> process.send(from, _)

  actor.continue(state)
}

// Handles a single publish command.
//
fn handle_publish(
  from,
  topic: String,
  body: String,
  opts: List(PublishOption),
  state: State,
) {
  case
    gnat_pub(state.nats, topic, body, opts)
    |> atom.to_string
  {
    "ok" -> process.send(from, Ok(Nil))
    _ -> process.send(from, Error(Unexpected))
  }
  actor.continue(state)
}

// Handles a single request command.
//
fn handle_request(
  from,
  topic: String,
  body: String,
  _opts: List(PublishOption),
  timeout: Int,
  state: State,
) {
  let opts = [
    #(atom.create("receive_timeout"), dynamic.int(timeout)),
  ]

  // In order to not block the connection actor we return a function
  // that will make the request.
  let req_func = fn() {
    case gnat_request(state.nats, topic, body, opts) {
      Ok(msg) ->
        decode_msg(msg)
        |> result.map_error(fn(_) { Unexpected })
      Error(err) ->
        case atom.to_string(err) {
          "timeout" -> Error(Timeout)
          "no_responders" -> Error(NoResponders)
          _ -> Error(Unexpected)
        }
    }
  }

  process.send(from, req_func)

  actor.continue(state)
}

// Handles a single unsubscribe command.
//
fn handle_unsubscribe(from, sid, state: State) {
  case
    gnat_unsub(state.nats, sid, [])
    |> atom.to_string
  {
    "ok" -> process.send(from, Ok(Nil))
    _ -> process.send(from, Error(Unexpected))
  }
  actor.continue(state)
}

// Handles a single subscribe command.
//
fn handle_subscribe(
  from,
  subscriber: process.Pid,
  topic: String,
  opts: List(SubscribeOption),
  state: State,
) {
  case gnat_sub(state.nats, subscriber, topic, opts) {
    Ok(sid) ->
      case process.link(subscriber) {
        True -> {
          process.send(from, Ok(sid))

          actor.continue(
            State(
              ..state,
              subscribers: dict.insert(state.subscribers, subscriber, sid),
            ),
          )
        }
        False -> {
          // TODO: unsub
          process.send(from, Error(Unexpected))

          actor.continue(state)
        }
      }
    Error(_) -> {
      process.send(from, Error(Unexpected))

      actor.continue(state)
    }
  }
}

//         //
// Publish //
//         //

fn take_reply_topic(prev: List(PublishOption), message: Message) {
  case message.reply_to {
    Some(rt) -> list.prepend(prev, ReplyTo(rt))
    None -> prev
  }
}

/// Publishes a single message to NATS on a provided topic.
///
pub fn publish(
  conn: Connection,
  topic: String,
  body: String,
  opts: List(PublishOption),
) {
  process.call(conn, 5000, Publish(_, topic, body, opts))
}

/// Publishes a single message to NATS using the data from a provided `Message`
/// record.
///
pub fn publish_message(conn: Connection, message: Message) {
  [Headers(dict.to_list(message.headers))]
  |> take_reply_topic(message)
  |> publish(conn, message.topic, message.body, _)
}

/// Sends a request and listens for a response synchronously.
/// When connection is established with option `EnableNoResponders`,
/// `Error(NoResponders)` will be returned immediately if no subscriber
/// exists for the topic.
///
/// See [request-reply pattern docs.](https://docs.nats.io/nats-concepts/core-nats/reqreply)
///
/// To handle a request from NATS see `handler.handle_request`.
///
pub fn request(
  conn: Connection,
  topic: String,
  body: String,
  opts: List(PublishOption),
  timeout: Int,
) {
  // Because Gnat's request function is blocking the connection actor will return
  // a function with all the data set in a clojure that calls the function.
  // This is done in order to not block the entire connection actor when waiting
  // for a response.
  let make_request =
    process.call(conn, 1000, Request(_, topic, body, opts, timeout))

  // Call the request function returned by the connection actor.
  make_request()
}

/// Sends a respond to a Message's reply_to topic.
///
pub fn respond(
  conn: Connection,
  message: Message,
  body: String,
  opts: List(PublishOption),
) {
  case message.reply_to {
    Some(rt) -> publish(conn, rt, body, opts)
    None -> Error(NoReplyTopic)
  }
}

//           //
// Subscribe //
//           //

fn subscription_mapper(
  conn: Connection,
  raw_msg: subscription.RawMessage,
) -> SubscriptionMessage {
  ReceivedMessage(
    conn: conn,
    sid: raw_msg.sid,
    status: raw_msg.status,
    message: Message(
      topic: raw_msg.topic,
      headers: raw_msg.headers,
      reply_to: raw_msg.reply_to,
      body: raw_msg.body,
    ),
  )
}

/// Subscribes to a NATS topic that can be received on the
/// provided OTP subject.
///
/// To subscribe as a member of a queue group, add option
/// `QueueGroup(String)` to list of options as the last
/// parameter.
///
/// ```gleam
/// subscribe(conn, subject, "my.topic", [QueueGroup("my-group")])
/// ```
///
/// See [Queue Groups docs.](https://docs.nats.io/nats-concepts/core-nats/queue)
///
pub fn subscribe(
  conn: Connection,
  subscriber: process.Subject(SubscriptionMessage),
  topic: String,
  opts: List(SubscribeOption),
) {
  case subscription.start_subscriber(conn, subscriber, subscription_mapper) {
    Ok(sub) -> {
      case process.call(conn, 5000, Subscribe(_, sub.pid, topic, opts)) {
        Ok(sid) -> {
          // unlink from subscription process
          process.unlink(sub.pid)

          Ok(sid)
        }
        Error(err) -> {
          // stop subscription actor
          process.kill(sub.pid)

          Error(err)
        }
      }
    }
    Error(_) -> Error(Unexpected)
  }
}

/// Unsubscribe from a subscription by providing the subscription ID.
///
pub fn unsubscribe(conn: Connection, sid: Int) {
  process.call(conn, 5000, Unsubscribe(_, sid))
}

//             //
// Server Info //
//             //

/// Returns server info provided by the connected NATS server.
///
pub fn server_info(conn: Connection) {
  process.call(conn, 5000, GetServerInfo)
}

//                      //
// Active Subscriptions //
//                      //

/// Returns the number of active subscriptions for the connection.
///
pub fn active_subscriptions(conn: Connection) {
  process.call(conn, 5000, GetActiveSubscriptions)
}

//           //
// New Inbox //
//           //

/// Returns a new random inbox.
///
pub fn new_inbox() {
  util.random_inbox("_INBOX.")
}

// Settings mapping helpers to create a map out of the settings
// type, which is expected by Gnat's start_link.

fn build_settings(
  host: String,
  port: Int,
  opts: List(ConnectionOption),
) -> dict.Dict(atom.Atom, dynamic.Dynamic) {
  [#("host", dynamic.string(host)), #("port", dynamic.int(port))]
  |> dict.from_list
  |> list.fold(opts, _, apply_conn_option)
  |> add_ssl_opts
  |> dict.take([
    "host", "port", "tls", "ssl_opts", "inbox_prefix", "connection_timeout",
    "no_responders", "username", "password", "token", "nkey_seed", "jwt",
  ])
  |> dict.to_list
  |> list.map(fn(i) { #(atom.create(i.0), i.1) })
  |> dict.from_list
}

fn apply_conn_option(
  prev: dict.Dict(String, dynamic.Dynamic),
  opt: ConnectionOption,
) {
  case opt {
    UserPass(user, pass) ->
      prev
      |> dict.insert("username", dynamic.string(user))
      |> dict.insert("password", dynamic.string(pass))
    Token(token) ->
      prev
      |> dict.insert("token", dynamic.string(token))
    NKeySeed(seed) ->
      prev
      |> dict.insert("nkey_seed", dynamic.string(seed))
    JWT(jwt) ->
      prev
      |> dict.insert("jwt", dynamic.string(jwt))
    CACert(path) ->
      prev
      |> dict.insert("tls", dynamic.bool(True))
      |> dict.insert("cacertfile", dynamic.string(path))
    ClientCert(cert, key) ->
      prev
      |> dict.insert("tls", dynamic.bool(True))
      |> dict.insert("certfile", dynamic.string(cert))
      |> dict.insert("keyfile", dynamic.string(key))
    InboxPrefix(prefix) ->
      dict.insert(prev, "inbox_prefix", dynamic.string(prefix))
    ConnectionTimeout(timeout) ->
      dict.insert(prev, "connection_timeout", dynamic.int(timeout))
    EnableNoResponders -> dict.insert(prev, "no_responders", dynamic.bool(True))
  }
}

fn add_ssl_opts(prev: dict.Dict(String, dynamic.Dynamic)) {
  dict.take(prev, ["cacertfile", "certfile", "keyfile"])
  |> dict.to_list
  |> list.map(fn(o) { #(atom.to_dynamic(atom.create(o.0)), o.1) })
  |> dynamic.properties
  |> dict.insert(prev, "ssl_opts", _)
}

// Decode Gnat message

// Decodes a message map returned by NATS
fn decode_msg(data: dynamic.Dynamic) {
  let decoder = {
    use topic <- decode.field(atom.create("Topic"), decode.string)
    use headers <- decode_headers
    use reply_to <- decode_reply_to
    use body <- decode.field(atom.create("Body"), decode.string)

    decode.success(Message(topic, headers, reply_to, body))
  }

  decode.run(data, decoder)
}

// Decodes headers from a map with message data.
// If the key is absent (which happens when no headers are sent)
// an empty map is returned.
fn decode_headers(next) {
  decode.optional_field(
    atom.create("Headers"),
    dict.new(),
    decode.dict(decode.string, decode.string),
    next,
  )
}

// Decodes reply_to from a map with message data into option.Option(String).
// If reply_to is `Nil` None is returned.
fn decode_reply_to(next) {
  decode.optional_field(
    atom.create("ReplyTo"),
    None,
    decode.optional(decode.string),
    next,
  )
}
