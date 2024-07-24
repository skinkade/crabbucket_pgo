import crabbucket/pgo.{HasRemainingTokens, remaining_tokens_for_key} as crabbucket
import gleam/dynamic
import gleam/erlang/process
import gleam/list
import gleam/option.{Some}
import gleam/otp/task
import gleam/pgo
import gleam/result
import gleeunit
import gleeunit/should

fn get_db() {
  let db =
    pgo.connect(
      pgo.Config(
        ..pgo.default_config(),
        host: "127.0.0.1",
        user: "postgres",
        password: Some("postgres"),
        database: "crabbucket_test",
        pool_size: 15,
      ),
    )

  let assert Ok(_) =
    pgo.execute(crabbucket.schema_migration_sql, db, [], dynamic.dynamic)
  let assert Ok(_) =
    pgo.execute(crabbucket.table_migration_sql, db, [], dynamic.dynamic)
  let assert Ok(_) =
    pgo.execute(crabbucket.index_migration_sql, db, [], dynamic.dynamic)
  db
}

pub fn main() {
  gleeunit.main()
}

pub fn insert_test() {
  let db = get_db()
  let window_duration_ms = 60 * 1000
  let default_remaining_tokens = 2
  let key = "test entry"

  let HasRemainingTokens(remaining1, _) =
    remaining_tokens_for_key(
      db,
      key,
      window_duration_ms,
      default_remaining_tokens,
    )
    |> should.be_ok()
  remaining1
  |> should.equal(default_remaining_tokens - 1)

  let HasRemainingTokens(remaining2, _) =
    remaining_tokens_for_key(
      db,
      key,
      window_duration_ms,
      default_remaining_tokens,
    )
    |> should.be_ok()
  remaining2
  |> should.equal(default_remaining_tokens - 2)

  remaining_tokens_for_key(
    db,
    key,
    window_duration_ms,
    default_remaining_tokens,
  )
  |> should.be_error()

  Nil
}

pub fn expiration_test() {
  let db = get_db()
  let window_duration_ms = 1000
  let default_remaining_tokens = 1
  let key = "test entry 2"

  let HasRemainingTokens(remaining1, _) =
    remaining_tokens_for_key(
      db,
      key,
      window_duration_ms,
      default_remaining_tokens,
    )
    |> should.be_ok()
  remaining1
  |> should.equal(default_remaining_tokens - 1)

  remaining_tokens_for_key(
    db,
    key,
    window_duration_ms,
    default_remaining_tokens,
  )
  |> should.be_error()

  process.sleep(1000)

  let HasRemainingTokens(remaining1, _) =
    remaining_tokens_for_key(
      db,
      key,
      window_duration_ms,
      default_remaining_tokens,
    )
    |> should.be_ok()
  remaining1
  |> should.equal(default_remaining_tokens - 1)
}

pub fn atomic_stress_test() {
  let db = get_db()
  let window_duration_ms = 60 * 1000
  let default_remaining_tokens = 100
  let key = "test entry 3"

  let results =
    list.range(1, 500)
    |> list.map(fn(_) {
      task.async(fn() {
        remaining_tokens_for_key(
          db,
          key,
          window_duration_ms,
          default_remaining_tokens,
        )
      })
    })
    |> list.map(task.await_forever)

  results
  |> list.count(fn(res) { result.is_ok(res) })
  |> should.equal(100)

  results
  |> list.count(fn(res) { result.is_error(res) })
  |> should.equal(400)
}

pub fn cleaner_test() {
  let db = get_db()
  let window_duration_ms = 1000
  let default_remaining_tokens = 100
  let key = "test entry 4"

  let HasRemainingTokens(remaining, _) =
    remaining_tokens_for_key(
      db,
      key,
      window_duration_ms,
      default_remaining_tokens,
    )
    |> should.be_ok()
  remaining
  |> should.equal(default_remaining_tokens - 1)

  let _ = crabbucket.create_and_start_cleaner(db, 1000)

  process.sleep(2000)

  let assert Ok(response) =
    pgo.execute(
      "SELECT NULL FROM crabbucket.token_buckets WHERE id = $1",
      db,
      [key |> pgo.text()],
      dynamic.dynamic,
    )

  response.rows
  |> should.equal([])
}
