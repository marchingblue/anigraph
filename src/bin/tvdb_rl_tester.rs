//! TVDB rate-limit tester.
//!
//! Empirically finds the safe maximum requests-per-second for the TVDB API.
//! Ramps up from `START_RATE` to `MAX_RATE`, sending `REQS_PER_STEP` requests
//! at each rate, and reports how many got rate-limited (429).
//!
//! # Usage
//! ```sh
//! TVDB_API_KEY=xxx cargo run --release --bin tvdb_rl_tester
//! TVDB_API_KEY=xxx cargo run --release --bin tvdb_rl_tester -- --start 5 --max 40 --step 2
//! ```

use std::time::{Duration, Instant};

use anyhow::Context;

const TVDB_BASE_URL: &str = "https://api4.thetvdb.com/v4";
const START_RATE: f64 = 2.0;
const MAX_RATE: f64 = 30.0;
const STEP: f64 = 1.0;
/// Number of requests to fire at each rate step.
const REQS_PER_STEP: usize = 20;
/// Series to use for test requests (One Piece).
const TVDB_SERIES_ID: i32 = 30991;

// ── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Debug)]
struct Args {
    start_rate: f64,
    max_rate: f64,
    step: f64,
    reqs_per_step: usize,
    series_id: i32,
    /// Sustained load: fire this many requests at the final safe rate.
    sustained: usize,
}

fn parse_args() -> Args {
    let mut args = std::env::args().skip(1).peekable();
    let mut out = Args {
        start_rate: START_RATE,
        max_rate: MAX_RATE,
        step: STEP,
        reqs_per_step: REQS_PER_STEP,
        series_id: TVDB_SERIES_ID,
        sustained: 0,
    };

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--start" => out.start_rate = args.next().and_then(|v| v.parse().ok()).unwrap_or(START_RATE),
            "--max" => out.max_rate = args.next().and_then(|v| v.parse().ok()).unwrap_or(MAX_RATE),
            "--step" => out.step = args.next().and_then(|v| v.parse().ok()).unwrap_or(STEP),
            "--reqs" => out.reqs_per_step = args.next().and_then(|v| v.parse().ok()).unwrap_or(REQS_PER_STEP),
            "--series" => out.series_id = args.next().and_then(|v| v.parse().ok()).unwrap_or(TVDB_SERIES_ID),
            "--sustained" => out.sustained = args.next().and_then(|v| v.parse().ok()).unwrap_or(0),
            "--help" | "-h" => {
                eprintln!("TVDB Rate Limit Tester");
                eprintln!();
                eprintln!("Options:");
                eprintln!("  --start RATE     Starting rate in req/s [default: {}]", START_RATE);
                eprintln!("  --max RATE       Maximum rate to test [default: {}]", MAX_RATE);
                eprintln!("  --step RATE      Rate increment per step [default: {}]", STEP);
                eprintln!("  --reqs N         Requests per rate step [default: {}]", REQS_PER_STEP);
                eprintln!(            "--series ID      TVDB series ID for test requests [default: {}]", TVDB_SERIES_ID);
                eprintln!("  --sustained N    Sustained-load: fire N requests at final safe rate [default: 0 = disabled]");
                eprintln!();
                eprintln!("Environment:");
                eprintln!("  TVDB_API_KEY     Required. Your TVDB v4 API key.");
                std::process::exit(0);
            }
            _ => {
                eprintln!("Unknown arg: {arg}. Use --help for usage.");
                std::process::exit(1);
            }
        }
    }

    out
}

// ── Main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = parse_args();
    let api_key = std::env::var("TVDB_API_KEY")
        .context("TVDB_API_KEY not set")?;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    // ── Authenticate ───────────────────────────────────────────────────
    println!("═══ TVDB Rate Limit Tester ═══");
    println!("Series ID: {}  |  Reqs per step: {}\n", args.series_id, args.reqs_per_step);

    let token = login(&client, &api_key).await?;
    println!("✓ Authenticated. Token: …{}", &token[token.len().saturating_sub(16)..]);

    // ── Pre-fetch a test endpoint path to use ─────────────────────────
    let test_url = format!("{TVDB_BASE_URL}/series/{}/episodes/default/1", args.series_id);
    // Verify the test endpoint works
    {
        let check = send_request(&client, &token, &test_url).await?;
        if check.status().is_success() {
            println!("✓ Test endpoint reachable: GET /series/{}/episodes/default/1\n", args.series_id);
        } else {
            let status = check.status();
            let body = check.text().await.unwrap_or_default().chars().take(80).collect::<String>();
            println!("⚠ Test endpoint returned HTTP {} — results may be affected by non-rate errors\n", status);
            println!("  Response: {body}");
        }
    }

    // ── Rate ramp ─────────────────────────────────────────────────────
    let mut rates_tested = 0u32;
    let mut rates_ok = 0u32;
    let mut best_rate = 0.0_f64;
    let mut all_ok = true;

    println!("{:<8} {:<10} {:<10} {:<10} {:<12} {:<12} {:<12} {:<10}",
        "Rate/s", "Sent", "✅ OK", "⛔ 429", "Min(ms)", "Avg(ms)", "Max(ms)", "Errors");
    println!("{:-<82}", "");

    let mut rate = args.start_rate;
    while rate <= args.max_rate + 0.001 && all_ok {
        let interval = Duration::from_secs_f64(1.0 / rate);

        let mut ok = 0u32;
        let mut rate_limited = 0u32;
        let mut other_errors = 0u32;
        let mut latencies = Vec::with_capacity(args.reqs_per_step);
        let mut last_error = String::new();

        for _ in 0..args.reqs_per_step {
            let start = Instant::now();
            let resp = send_request(&client, &token, &test_url).await;
            let elapsed = start.elapsed();

            match resp {
                Ok(r) if r.status().is_success() => {
                    ok += 1;
                    latencies.push(elapsed);
                }
                Ok(r) if r.status().as_u16() == 429 => {
                    rate_limited += 1;
                }
                Ok(r) => {
                    other_errors += 1;
                    let status = r.status();
                    let body = r.text().await.unwrap_or_default().chars().take(60).collect::<String>();
                    last_error = format!("HTTP {}: {}", status, body);
                }
                Err(e) => {
                    other_errors += 1;
                    last_error = e.to_string().chars().take(60).collect();
                }
            }

            // Wait for the remainder of the interval
            let elapsed = start.elapsed();
            if elapsed < interval {
                tokio::time::sleep(interval - elapsed).await;
            }
        }

        let min_ms = latencies.iter().min().map(|d| d.as_secs_f64() * 1000.0).unwrap_or(0.0);
        let avg_ms = if !latencies.is_empty() {
            latencies.iter().map(|d| d.as_secs_f64()).sum::<f64>() / latencies.len() as f64 * 1000.0
        } else {
            0.0
        };
        let max_ms = latencies.iter().max().map(|d| d.as_secs_f64() * 1000.0).unwrap_or(0.0);

        let error_str = if other_errors > 0 {
            format!("{} errs", other_errors)
        } else if !last_error.is_empty() {
            last_error.chars().take(8).collect()
        } else {
            "-".to_string()
        };

        print!("{:<8.1} {:<10} {:<10} {:<10} {:<12.1} {:<12.1} {:<12.1} {:<10}",
            rate, args.reqs_per_step, ok, rate_limited, min_ms, avg_ms, max_ms, error_str);

        if rate_limited > 0 {
            all_ok = false;
            println!("  ⚠ RATE LIMITED");
        } else if other_errors > 0 {
            println!("  ⚠ NON-RATE ERRORS");
        } else {
            println!("  ✓");
            best_rate = rate;
            rates_ok += 1;
        }

        rates_tested += 1;
        rate += args.step;
    }

    // ── Summary ───────────────────────────────────────────────────────
    println!("\n{:=<82}", "");
    println!(" RESULTS");
    println!("{:=<82}", "");

    if all_ok {
        println!("  No rate limiting detected up to {:.1} req/s", args.max_rate);
        println!("  Safe max rate: ≥{:.1} req/s (untested beyond)", args.max_rate);
    } else {
        println!("  Rate limiting detected at {:.1} req/s", rate - args.step);
        println!("  Last fully-safe rate: {:.1} req/s", best_rate);
        println!();
        println!("  Recommendation: use {:.1} req/s (80% of {:.1})",
            best_rate * 0.8, best_rate);
    }

    println!();
    println!("  Rates tested:  {rates_tested}");
    println!("  Fully OK:      {rates_ok}");
    println!("  Series used:   {} (One Piece)", args.series_id);

    // ── Sustained load test ────────────────────────────────────────────
    if args.sustained > 0 {
        let test_rate = if all_ok { args.max_rate } else { best_rate };
        println!("\n{:=<82}", "");
        println!(" SUSTAINED LOAD TEST — {} requests at {:.1} req/s", args.sustained, test_rate);
        println!("{:=<82}", "");

        let interval = Duration::from_secs_f64(1.0 / test_rate);
        let mut ok = 0u32;
        let mut rate_limited = 0u32;
        let mut other_errors = 0u32;
        let mut latencies = Vec::with_capacity(args.sustained);

        // Progress bar (simple dot-based)
        let dot_interval = (args.sustained / 50).max(1); // ~50 dots

        for i in 0..args.sustained {
            let start = Instant::now();
            let resp = send_request(&client, &token, &test_url).await;
            let elapsed = start.elapsed();

            match resp {
                Ok(r) if r.status().is_success() => {
                    ok += 1;
                    latencies.push(elapsed);
                }
                Ok(r) if r.status().as_u16() == 429 => {
                    rate_limited += 1;
                    // Print progress on failure to make it visible immediately
                    print!("⛔");
                    std::io::Write::flush(&mut std::io::stdout()).ok();
                }
                Ok(_r) => {
                    other_errors += 1;
                    print!("⚠");
                    std::io::Write::flush(&mut std::io::stdout()).ok();
                }
                Err(_) => {
                    other_errors += 1;
                    print!("⚠");
                    std::io::Write::flush(&mut std::io::stdout()).ok();
                }
            }

            // Print progress dot
            if (i + 1) % dot_interval == 0 {
                print!(".");
                std::io::Write::flush(&mut std::io::stdout()).ok();
            }

            // Wait for the remainder of the interval
            let elapsed = start.elapsed();
            if elapsed < interval {
                tokio::time::sleep(interval - elapsed).await;
            }
        }
        println!(); // newline after progress

        // ── Sustained results ───────────────────────────────────────────
        let min_ms = latencies.iter().min().map(|d| d.as_secs_f64() * 1000.0).unwrap_or(0.0);
        let avg_ms = if !latencies.is_empty() {
            latencies.iter().map(|d| d.as_secs_f64()).sum::<f64>() / latencies.len() as f64 * 1000.0
        } else {
            0.0
        };
        let max_ms = latencies.iter().max().map(|d| d.as_secs_f64() * 1000.0).unwrap_or(0.0);

        println!();
        println!("  {:<6} / {} OK  |  {} ⛔ 429  |  {} ⚠ errors", ok, args.sustained, rate_limited, other_errors);
        println!("  Min: {:<8.1}ms  |  Avg: {:<8.1}ms  |  Max: {:<8.1}ms", min_ms, avg_ms, max_ms);

        if rate_limited == 0 && other_errors == 0 {
            println!("  ✅ Sustained load test PASSED at {:.1} req/s — rate is safe under sustained load", test_rate);
        } else {
            println!("  ❌ Sustained load test FAILED at {:.1} req/s — reduce rate and retry", test_rate);
            if rate_limited > 0 {
                println!("     {rate_limited} requests were rate-limited (429).");
                println!("     Try {:.1} req/s as your new max.", test_rate * 0.7);
            }
            if other_errors > 0 {
                println!("     {other_errors} non-rate errors occurred.");
            }
        }
    }

    Ok(())
}

// ── Helpers ─────────────────────────────────────────────────────────────────

async fn login(client: &reqwest::Client, api_key: &str) -> anyhow::Result<String> {
    let url = format!("{TVDB_BASE_URL}/login");
    let resp = client
        .post(&url)
        .header("Content-Type", "application/json")
        .body(format!(r#"{{"apikey":"{api_key}"}}"#))
        .send()
        .await
        .context("TVDB login request failed")?;

    if !resp.status().is_success() {
        let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                anyhow::bail!("TVDB login failed: HTTP {}: {}", status, body);
            }

    #[derive(serde::Deserialize)]
    struct LoginResp {
        data: LoginData,
    }
    #[derive(serde::Deserialize)]
    struct LoginData {
        token: String,
    }

    let body: LoginResp = resp.json().await.context("TVDB login parse failed")?;
    Ok(body.data.token)
}

async fn send_request(client: &reqwest::Client, token: &str, url: &str) -> reqwest::Result<reqwest::Response> {
    client
        .get(url)
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
}
