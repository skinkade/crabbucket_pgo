import crabbucket/pgo as crabbucket
import envoy
import gleam/dynamic
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/list
import gleam/option
import gleam/pgo
import mist
import wisp

type Context {
  Context(db: pgo.Connection)
}

fn set_header_if_not_present(resp: wisp.Response, name: String, value: String) {
  case list.key_find(resp.headers, name) {
    Ok(_) -> resp
    Error(_) -> resp |> wisp.set_header(name, value)
  }
}

const limit_header = "x-rate-limit-limit"

const remaining_header = "x-rate-limit-remaining"

const reset_header = "x-rate-limit-reset"

fn with_rate_limit(
  conn: pgo.Connection,
  key: String,
  window_duration_ms: Int,
  default_token_count: Int,
  handler: fn() -> wisp.Response,
) -> wisp.Response {
  let limit_result =
    crabbucket.remaining_tokens_for_key(
      conn,
      key,
      window_duration_ms,
      default_token_count,
    )

  case limit_result {
    Error(crabbucket.PgoError(e)) -> {
      io.debug(e)
      wisp.internal_server_error()
    }
    Error(crabbucket.MustWaitUntil(next_reset)) -> {
      wisp.response(429)
      |> wisp.set_header(limit_header, default_token_count |> int.to_string())
      |> wisp.set_header(remaining_header, "0")
      |> wisp.set_header(reset_header, next_reset |> int.to_string())
    }
    Ok(crabbucket.HasRemainingTokens(tokens, next_reset)) -> {
      handler()
      |> set_header_if_not_present(
        limit_header,
        default_token_count |> int.to_string(),
      )
      |> set_header_if_not_present(remaining_header, tokens |> int.to_string())
      |> set_header_if_not_present(reset_header, next_reset |> int.to_string())
    }
  }
}

const api_rate_limit_window_duration_ms = 60_000

const global_api_limit_per_minute = 100

const secure_api_limit_per_minute = 10

fn secure_handler(_req: wisp.Request, ctx: Context, user_id: String) {
  use <- with_rate_limit(
    ctx.db,
    "api_secure:user:" <> user_id,
    api_rate_limit_window_duration_ms,
    secure_api_limit_per_minute,
  )
  wisp.ok()
}

fn api_handler(req: wisp.Request, ctx: Context) {
  let user_id = "12345"
  use <- with_rate_limit(
    ctx.db,
    "api:user:" <> user_id,
    api_rate_limit_window_duration_ms,
    global_api_limit_per_minute,
  )

  case wisp.path_segments(req) |> list.drop(1) {
    ["get-something"] -> wisp.ok()
    ["secure"] -> secure_handler(req, ctx, user_id)
    _ -> wisp.not_found()
  }
}

fn handle_request(req: wisp.Request, ctx: Context) {
  case wisp.path_segments(req) {
    ["api", ..] -> api_handler(req, ctx)
    _ -> wisp.not_found()
  }
}

pub fn main() {
  wisp.configure_logger()
  let secret_key_base = wisp.random_string(64)

  let assert Ok(database_host) = envoy.get("DATABASE_HOST")
  let assert Ok(database_name) = envoy.get("DATABASE_NAME")
  let assert Ok(database_user) = envoy.get("POSTGRES_USER")
  let database_password =
    envoy.get("POSTGRES_PASSWORD")
    |> option.from_result()

  let db =
    pgo.connect(
      pgo.Config(
        ..pgo.default_config(),
        host: database_host,
        user: database_user,
        password: database_password,
        database: database_name,
        pool_size: 15,
      ),
    )
  let context = Context(db: db)

  // Configure database if needed and start cleaner
  let assert Ok(_) =
    pgo.execute(crabbucket.schema_migration_sql, db, [], dynamic.dynamic)
  let assert Ok(_) =
    pgo.execute(crabbucket.table_migration_sql, db, [], dynamic.dynamic)
  let assert Ok(_) =
    pgo.execute(crabbucket.index_migration_sql, db, [], dynamic.dynamic)
  let _token_bucket_cleaner = crabbucket.create_and_start_cleaner(db, 1000 * 60)

  let assert Ok(_) =
    wisp.mist_handler(handle_request(_, context), secret_key_base)
    |> mist.new
    |> mist.port(8000)
    |> mist.start_http

  process.sleep_forever()
}
