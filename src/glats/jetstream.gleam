import glats
import gleam/int

/// Errors that can be returned when working with Jetstream.
///
pub type JetstreamError {
  // code: 10039
  JetstreamNotEnabledForAccount(String)
  // code: 10076
  JetstreamNotEnabled(String)
  // code: 10023
  InsufficientResources(String)
  // code: 10052
  InvalidStreamConfig(String)
  // code: 10056
  StreamNameInSubjectDoesNotMatch(String)
  // code: 10058
  StreamNameInUse(String)
  // code: 10059
  StreamNotFound(String)
  // code: 10110
  StreamPurgeNotAllowed(String)
  // code: 10037
  NoMessageFound(String)
  // code: 10014
  ConsumerNotFound(String)
  // code: 10013
  ConsumerNameExists(String)
  // code: 10105
  ConsumerAlreadyExists(String)
  // code: 10071
  WrongLastSequence(String)
  // code: 10003
  BadRequest(String)
  Unknown(Int, String)
  DecodeError(String)
  Timeout
  NoResponders
  PullConsumerRequired(String)
}

/// Sets the storage type in a stream.
///
pub type StorageType {
  FileStorage
  MemoryStorage
}

/// Sends an acknowledgement for a message.
///
pub fn ack(conn: glats.Connection, message: glats.Message) {
  glats.respond(conn, message, "", [])
}

/// Sends a term acknowledgement for a message.
///
/// Instructs the server to stop redelivery of a message without acknowledging
/// it as successfully processed.
///
pub fn term(conn: glats.Connection, message: glats.Message) {
  glats.respond(conn, message, "+TERM", [])
}

/// Sends a negative acknowledgement for a message.
///
/// Delivery will be retried until ack'd or term'd.
///
pub fn nack(conn: glats.Connection, message: glats.Message) {
  glats.respond(conn, message, "-NAK", [])
}

/// Sends a negative acknowledgement for a message and delays
/// redelivery of the message. The unit is in nanoseconds.
///
pub fn nack_delay(
  conn: glats.Connection,
  message: glats.Message,
  with delay: Int,
) {
  glats.respond(
    conn,
    message,
    "-NAK {\"delay\":" <> int.to_string(delay) <> "}",
    [],
  )
}
