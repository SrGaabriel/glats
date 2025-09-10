import gleam/dict
import gleam/dynamic
import gleam/dynamic/decode
import gleam/erlang/atom
import gleam/erlang/process
import gleam/io
import gleam/option.{None}
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
  actor.new_with_initialiser(1000, fn(my_subject) {
    // Monitor subscriber process.
    let _monitor =
      process.monitor(
        receiver
        |> process.subject_owner
        |> result.unwrap(process.self()),
      )

    let selector =
      process.new_selector()
      |> process.select_monitors(fn(down) {
        case down {
          process.ProcessDown(_, _, _) -> SubscriberExited
          _ -> SubscriberExited
        }
      })
      |> process.select_other(fn(data) { map_gnat_message(data) })

    actor.initialised(State(conn, receiver, mapper))
    |> actor.selecting(selector)
    |> actor.returning(my_subject)
    |> Ok
  })
  |> actor.on_message(loop)
  |> actor.start
}

fn map_gnat_message(data: dynamic.Dynamic) -> Message {
  data
  |> decode_raw_msg
  |> result.map(ReceivedMessage)
  |> result.unwrap(DecodeError(data))
}

fn loop(state: State(a, b), message: Message) {
  case message {
    ReceivedMessage(raw_msg) -> {
      actor.send(state.receiver, state.mapper(state.conn, raw_msg))
      actor.continue(state)
    }
    DecodeError(data) -> {
      io.println("failed to decode: " <> string.inspect(data))
      actor.continue(state)
    }
    SubscriberExited -> {
      io.println("subscriber exited")
      actor.stop()
    }
  }
}

// Decode Gnat message

// Decodes a message map returned by NATS
pub fn decode_raw_msg(data: dynamic.Dynamic) {
  let decoder = {
    use sid <- decode_sid
    use status <- decode_status
    use topic <- decode.field(atom.create("topic"), decode.string)
    use headers <- decode_headers
    use reply_to <- decode_reply_to
    use body <- decode.field(atom.create("body"), decode.string)

    decode.success(RawMessage(sid, status, topic, headers, reply_to, body))
  }

  decode.run(data, decoder)
}

// Decodes sid with default value of -1 if not found.
fn decode_sid(next) {
  decode.optional_field(atom.create("sid"), -1, decode.int, next)
}

// Decodes status.
fn decode_status(next) {
  decode.optional_field(
    atom.create("status"),
    None,
    decode.optional(decode.int),
    next,
  )
}

// Decodes headers from a map with message data.
// If the key is absent (which happens when no headers are sent)
// an empty map is returned.
fn decode_headers(next) {
  decode.optional_field(
    atom.create("headers"),
    dict.new(),
    decode.dict(decode.string, decode.string),
    next,
  )
}

// Decodes reply_to from a map with message data into option.Option(String).
// If reply_to is `Nil` None is returned.
fn decode_reply_to(next) {
  decode.optional_field(
    atom.create("reply_to"),
    None,
    decode.optional(decode.string),
    next,
  )
}
