import gleam/dynamic
import gleam/erlang/process.{type Subject}
import gleam/io
import gleam/otp/actor
import gleam/pgo.{type Connection}
import gleam/result

// These need to be separate statements for pgo.execute()

pub const schema_migration_sql = "CREATE SCHEMA IF NOT EXISTS crabbucket;"

pub const table_migration_sql = "
CREATE UNLOGGED TABLE IF NOT EXISTS crabbucket.token_buckets (
    id TEXT PRIMARY KEY,
    window_end BIGINT NOT NULL,
    remaining_tokens INT NOT NULL
);
"

pub const index_migration_sql = "
CREATE INDEX IF NOT EXISTS IX_token_bucket_window_end
ON crabbucket.token_buckets (window_end);
"

@external(erlang, "os", "system_time")
fn system_time(second_division: Int) -> Int

fn now_ms() -> Int {
  system_time(1000)
}

pub type RemainingTokenCountSuccess {
  HasRemainingTokens(remaining_tokens: Int, next_reset_timestamp: Int)
}

pub type RemainingTokenCountFailure {
  MustWaitUntil(next_reset_timestamp: Int)
  PgoError(error: pgo.QueryError)
}

/// Takes an arbitrary key and window duration, inserting a record if non-existing.
/// 
/// Suggestion: you may wish to format your key such that it indicates
/// usage / purpose, value type, and value.
/// For instance, if you're limiting a specific endpoint for a given user,
/// the key might look something like:
/// `some_endpoint_name:user:12345`
/// 
/// `window_duration_milliseconds` describes the length of the time window
/// valid for a number of tokens.
/// For instance, a value of 1,000 with a default token count of 100 would mean
/// an action could occur 100 times per second.
/// 
/// Return should be either
/// `HasRemainingTokens(remaining_tokens: Int, next_reset_timestamp: Int)`,
/// which indicates that an action may proceed and contains how many more times
/// the action may occur with the current window,
/// or it may be `MustWaitUntil(next_reset_timestamp: Int)`,
/// which indicates that no tokens remain for this key and the next Unix
/// timestamp (in milliseconds) that the client must wait for.
pub fn remaining_tokens_for_key(
  conn: Connection,
  key: String,
  window_duration_ms: Int,
  default_tokens: Int,
) -> Result(RemainingTokenCountSuccess, RemainingTokenCountFailure) {
  let sql =
    "
      INSERT INTO crabbucket.token_buckets AS tb
      (id, window_end, remaining_tokens)
      VALUES
      ($1, $3, $4)
      ON CONFLICT (id)
      DO UPDATE SET
          window_end = CASE
              WHEN tb.window_end < $2
              THEN EXCLUDED.window_end
              ELSE tb.window_end
          END,
          remaining_tokens = CASE
              WHEN tb.window_end < $2
              THEN EXCLUDED.remaining_tokens
              ELSE GREATEST(-1, tb.remaining_tokens - 1)
          END
      RETURNING tb.remaining_tokens, tb.window_end;
    "

  let window_start = now_ms()
  let window_end = window_start + window_duration_ms

  use response <- result.try({
    pgo.execute(
      sql,
      conn,
      [
        key |> pgo.text(),
        window_start |> pgo.int(),
        window_end |> pgo.int(),
        default_tokens - 1 |> pgo.int(),
      ],
      dynamic.tuple2(dynamic.int, dynamic.int),
    )
    |> result.map_error(fn(e) { PgoError(e) })
  })

  let assert [#(remaining_tokens, window_end)] = response.rows

  case remaining_tokens < 0 {
    True -> Error(MustWaitUntil(window_end))
    False -> Ok(HasRemainingTokens(remaining_tokens, window_end))
  }
}

pub type TokenBucketCleanerMessage(msg) {
  ShutdownTokenBucketCleaner
  StartTokenBucketCleaner(Subject(TokenBucketCleanerMessage(msg)))
  RunTokenBucketCleaner(Subject(TokenBucketCleanerMessage(msg)))
}

pub type TokenBucketCleanerState(key) {
  TokenBucketCleanerState(conn: Connection, sweep_interval_ms: Int)
}

pub fn handle_cleaner_message(
  message: TokenBucketCleanerMessage(msg),
  state: TokenBucketCleanerState(key),
) -> actor.Next(TokenBucketCleanerMessage(msg), TokenBucketCleanerState(key)) {
  case message {
    ShutdownTokenBucketCleaner -> actor.Stop(process.Normal)

    StartTokenBucketCleaner(subject) -> {
      process.send_after(
        subject,
        state.sweep_interval_ms,
        RunTokenBucketCleaner(subject),
      )
      actor.continue(state)
    }

    RunTokenBucketCleaner(subject) -> {
      let sql =
        "
          DO
          $$
          DECLARE
              now_ms BIGINT := EXTRACT(EPOCH FROM NOW()) * 1000;
              max_iterations INT := 10;
              total_count BIGINT;
              iterations INT;
          BEGIN
              total_count := (
                SELECT COUNT(1)
                FROM crabbucket.token_buckets
                WHERE window_end <= now_ms
              );

              iterations := FLOOR(total_count / 1000) + 1;
              iterations := LEAST(iterations, max_iterations);
              
              FOR i IN 1..iterations LOOP
                  DELETE FROM crabbucket.token_buckets
                  WHERE id IN (
                    SELECT id
                    FROM crabbucket.token_buckets
                    WHERE window_end <= now_ms
                    ORDER BY window_end
                    LIMIT 1000
                  );
              END LOOP;
          END;
          $$;
        "

      let response = pgo.execute(sql, state.conn, [], dynamic.dynamic)

      case response {
        Error(e) -> {
          io.debug(e)
          Nil
        }
        _ -> Nil
      }

      process.send_after(
        subject,
        state.sweep_interval_ms,
        RunTokenBucketCleaner(subject),
      )
      actor.continue(state)
    }
  }
}

/// Having our token buckets in a database table means there's going to be
/// cruft left behind.
/// To mitigate this, a TokenBucketCleaner is an actor which periodically executes
/// a SQL command to purge expired buckets.
/// 
/// `sweep_interval_ms` describes how often the script should run,
/// in milliseconds.
/// 
/// The cleaner deletes 1,000-record segments at a time to help avoid writer
/// contention in the database, and is hard-coded to only delete up to
/// 10 segments (10,000 records) at a time,
/// to avoid holding onto a pool connection for too long.
pub fn create_and_start_cleaner(conn: Connection, sweep_interval_ms: Int) {
  let assert Ok(cleaner) =
    actor.start(
      TokenBucketCleanerState(conn: conn, sweep_interval_ms: sweep_interval_ms),
      handle_cleaner_message,
    )
  actor.send(cleaner, StartTokenBucketCleaner(cleaner))
  cleaner
}
