//// Request handler will handle receiving messages from a subscription,
//// pass the data to a callback and then reply to NATS with the returned
//// data.
////
//// ## Example
////
//// ```gleam
//// import gleam/option.{None}
//// import gleam/result
//// import gleam/erlang/process
//// import glats
//// import glats/handler.{Reply, Request, Response}
//// 
//// pub fn main() {
////   use conn <- result.then(glats.connect("localhost", 4222, []))
//// 
////   // Start a request handler actor that will call `ping_pong_handler` for
////   // every request received from NATS topic "do.ping".
////   assert Ok(_actor) =
////     handler.handle_request(conn, [], "do.ping", None, ping_pong_handler)
//// 
////   process.sleep_forever()
//// 
////   Ok(Nil)
//// }
//// 
//// pub fn ping_pong_handler(req: Request, state) {
////   // Got message: Hello
////   io.println("Got message: " <> req.body)
////
////   // Reply with a message with the same headers and append to body.
////   Reply(
////     Response(
////       headers: req.headers,
////       reply_to: None,
////       body: req.body <> " from glats!",
////     ),
////     state,
////   )
//// }
//// ```
////
//// Then in a shell with `natscli`.
////
//// ```sh
//// $ nats req do.ping 'Hello'
//// 12:16:47 Sending request on "do.ping"
//// 12:16:47 Received with rtt 427.64µs
//// Hello from glats!
//// ```
////

import glats
import gleam/dict
import gleam/erlang/process
import gleam/option.{None, Some}
import gleam/otp/actor
import gleam/string

/// The message data received from the request handler's topic.
///
pub type Request {
  Request(headers: dict.Dict(String, String), body: String)
}

/// The message data that should be replied to the requester.
///
pub type Response {
  Response(
    headers: dict.Dict(String, String),
    reply_to: option.Option(String),
    body: String,
  )
}

/// Next step for the request handler to do.
///
pub type RequestOutcome(a) {
  /// The request handler will reply to the requester with the
  /// response and save the state.
  ///
  Reply(response: Response, state: a)
  /// The request handler will stop with provided exit reason.
  ///
  Stop(process.ExitReason)
}

/// The request handling callback that should be passed to the request handler.
/// This will be called for every request received.
pub type RequestHandler(a) =
  fn(Request, a) -> RequestOutcome(a)

type RequestHandlerState(a) {
  RequestHandlerState(
    conn: glats.Connection,
    sid: Int,
    handler: RequestHandler(a),
    inner: a,
  )
}

/// Starts an actor that subscribes to the desired NATS topic and calls the
/// provided request handler with the request data and replies to NATS with
/// the returned message data from the request handler.
///
pub fn handle_request(
  conn: glats.Connection,
  state: a,
  topic: String,
  opts: List(glats.SubscribeOption),
  handler: RequestHandler(a),
) {
  actor.new_with_initialiser(5000, fn(my_subject) {
    let subscriber = process.new_subject()
    let selector =
      process.new_selector()
      |> process.select(subscriber)

    case glats.subscribe(conn, subscriber, topic, opts) {
      Ok(sid) -> {
        actor.initialised(RequestHandlerState(conn, sid, handler, state))
        |> actor.selecting(selector)
        |> actor.returning(my_subject)
        |> Ok
      }
      Error(err) -> Error(string.inspect(err))
    }
  })
  |> actor.on_message(request_handler_loop)
  |> actor.start
}

fn request_handler_loop(
  state: RequestHandlerState(a),
  message: glats.SubscriptionMessage,
) {
  case message {
    glats.ReceivedMessage(conn, _, _, msg) ->
      request_handler_msg(conn, msg, state)
  }
}

fn request_handler_msg(
  conn: glats.Connection,
  msg: glats.Message,
  state: RequestHandlerState(a),
) {
  case msg.reply_to {
    Some(reply_to) -> {
      let req = Request(msg.headers, msg.body)

      case state.handler(req, state.inner) {
        Reply(res, new_inner) -> {
          let pub_res =
            glats.publish_message(
              conn,
              glats.Message(
                topic: reply_to,
                headers: res.headers,
                reply_to: res.reply_to,
                body: res.body,
              ),
            )

          case pub_res {
            Ok(Nil) ->
              actor.continue(RequestHandlerState(..state, inner: new_inner))
            Error(err) -> actor.stop_abnormal(string.inspect(err))
          }
        }

        Stop(_reason) -> actor.stop()
      }
    }
    None -> actor.continue(state)
  }
}
