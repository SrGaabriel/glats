import gleam/dict
import gleam/dynamic
import gleam/erlang/atom
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/option.{None, Some}
import gleam/otp/actor
import gleam/result
import gleam/string

/// Raw message received from Gnat.
pub type RawMessage {
  RawMessage(
    sid: Int,
    status: option.Option(Int),
    topic: String,
    headers: dict.Dict(String, String),
    reply_to: option.Option(String),
    body: String,
  )
}

pub type MapperFunc(a, b) =
  fn(a, RawMessage) -> b

type State(a, b) {
  State(conn: a, receiver: process.Subject(b), mapper: MapperFunc(a, b))
}

pub opaque type Message {
  ReceivedMessage(RawMessage)
  DecodeError(dynamic.Dynamic)
  SubscriberExited
}

pub fn start_subscriber(
  conn: a,
  receiver: process.Subject(b),
  mapper: MapperFunc(a, b),
) {
  actor.start_spec(actor.Spec(
    init: fn() {
      // Monitor subscriber process.
      let monitor =
        process.monitor_process(
          receiver
          |> process.subject_owner,
        )

      let selector =
        process.new_selector()
        |> process.selecting_process_down(monitor, fn(_) { SubscriberExited })
        |> process.selecting_record2(
          atom.create_from_string("msg"),
          map_gnat_message,
        )

      actor.Ready(State(conn, receiver, mapper), selector)
    },
    init_timeout: 1000,
    loop: loop,
  ))
}

fn map_gnat_message(data: dynamic.Dynamic) -> Message {
  data
  |> decode_raw_msg
  |> result.map(ReceivedMessage)
  |> result.unwrap(DecodeError(data))
}

fn loop(message: Message, state: State(a, b)) {
  case message {
    ReceivedMessage(raw_msg) -> {
      actor.send(state.receiver, state.mapper(state.conn, raw_msg))
      actor.Continue(state, None)
    }
    DecodeError(data) -> {
      io.println("failed to decode: " <> string.inspect(data))
      actor.Continue(state, None)
    }
    SubscriberExited -> {
      io.println("subscriber exited")
      actor.Stop(process.Normal)
    }
  }
}

// Decode Gnat message

// Decodes a message map returned by NATS
pub fn decode_raw_msg(data: dynamic.Dynamic) {
  data
  |> dynamic.decode6(
    RawMessage,
    sid,
    status,
    atom_field("topic", dynamic.string),
    headers,
    reply_to,
    atom_field("body", dynamic.string),
  )
}

// Decodes sid with default value of -1 if not found.
fn sid(data: dynamic.Dynamic) {
  data
  |> atom_field("sid", dynamic.int)
  |> result.or(Ok(-1))
}

// Decodes status with default value of -1 if not found.
fn status(data: dynamic.Dynamic) {
  data
  |> atom_field("status", dynamic.string)
  |> result.map(fn(s) {
    int.parse(s)
    |> result.map(Some)
    |> result.unwrap(None)
  })
  |> result.or(Ok(None))
}

// Decodes headers from a map with message data.
// If the key is absent (which happens when no headers are sent)
// an empty map is returned.
fn headers(data: dynamic.Dynamic) {
  data
  |> atom_field(
    "headers",
    dynamic.list(dynamic.tuple2(dynamic.string, dynamic.string)),
  )
  |> result.map(dict.from_list)
  |> result.or(Ok(dict.new()))
}

// Decodes reply_to from a map with message data into option.Option(String).
// If reply_to is `Nil` None is returned.
fn reply_to(data: dynamic.Dynamic) {
  data
  |> dynamic.optional(atom_field("reply_to", dynamic.string))
  |> result.or(Ok(None))
}

fn atom_field(key: String, value) {
  dynamic.field(atom.create_from_string(key), value)
}
