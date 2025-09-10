import glats
import gleam/result

pub fn main() {
  use started <- result.try(glats.connect("localhost", 4222, []))
  let conn = started.data

  // Publish a single message to "some.subject".
  let assert Ok(Nil) = glats.publish(conn, "some.subject", "hello world!", [])

  Ok(Nil)
}
