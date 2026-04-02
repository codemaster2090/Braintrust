#!/usr/bin/env node
"use strict";

const fs = require("fs");
const fsp = fs.promises;
const http = require("http");
const https = require("https");
const path = require("path");
const { execFile } = require("child_process");
const { promisify } = require("util");
let MongoClient;
const execFileAsync = promisify(execFile);
let SocksProxyAgentClass = null;

try {
  ({ MongoClient } = require("mongodb"));
} catch (error) {
  MongoClient = null;
}

const BASE_URL = "https://app.usebraintrust.com";
const API_PREFIX = "/api/freelancers/";
const DEFAULT_CONCURRENCY = 2;
const DEFAULT_TIMEOUT_MS = 30000;
const DEFAULT_RETRIES = 5;
const DEFAULT_RETRY_BASE_MS = 1000;
const DEFAULT_LOG_EVERY = 25;
const DEFAULT_MAX_ERRORS = 1000;
const DEFAULT_MIN_REQUEST_GAP_MS = 900;
const DEFAULT_MAX_REQUEST_GAP_MS = 20000;
const DEFAULT_RATE_LIMIT_COOLDOWN_MS = 60000;
const DEFAULT_MAX_RATE_LIMIT_COOLDOWN_MS = 900000;
const DEFAULT_TRANSIENT_ERROR_COOLDOWN_MS = 10000;
const DEFAULT_RECOVERY_SUCCESS_COUNT = 25;
const DEFAULT_RESUME_LOOKBACK = 20;
const DEFAULT_MULLVAD_COMMAND = process.platform === "win32" ? "mullvad.exe" : "mullvad";
const DEFAULT_MULLVAD_ROTATE_ON_RATE_LIMIT = true;
const DEFAULT_MULLVAD_RECONNECT_ATTEMPTS = 4;
const DEFAULT_MULLVAD_RECONNECT_TIMEOUT_MS = 120000;
const DEFAULT_MULLVAD_SETTLE_MS = 5000;
const DEFAULT_MULLVAD_PROXY_MODE = true;
const DEFAULT_MULLVAD_PROXY_HOST = "10.64.0.1";
const DEFAULT_MULLVAD_PROXY_PORT = 1080;
const DEFAULT_MULLVAD_PROXY_COUNTRY = "auto";
const DEFAULT_MULLVAD_PROXY_MAX_RELAYS = 32;
const DEFAULT_MULLVAD_PROXY_MAX_INFLIGHT = 2;
const DEFAULT_HIGH_SPEED_MODE = false;
const DEFAULT_MONGO_URI =
  process.env.MONGODB_URI || process.env.MONGO_URI || "mongodb://localhost:27017/";
const DEFAULT_MONGO_DB = process.env.MONGO_DB || "braintrust";
const DEFAULT_MONGO_COLLECTION = process.env.MONGO_COLLECTION || "profiles";
const DEFAULT_MONGO_TIMEOUT_MS = 10000;
const USER_AGENT = "BraintrustProfileDownloader/1.0";

async function main() {
  const options = parseArgs(process.argv.slice(2));

  if (options.help) {
    printHelp();
    return;
  }

  const inputPath = options.input || detectDefaultInput();
  if (!inputPath) {
    throw new Error(
      "No input CSV provided. Pass --input <file.csv> or place a filtered Braintrust CSV in this folder."
    );
  }

  const resolvedInputPath = path.resolve(inputPath);
  const outputDir = path.resolve(options.out || "braintrust_profile_details");
  const profilesRoot = path.join(outputDir, "profiles");
  const errorsFile = path.join(outputDir, "errors.ndjson");
  const progressFile = path.join(outputDir, "progress.json");
  const manifestFile = path.join(outputDir, "manifest.json");
  const detailsNdjsonFile = path.join(outputDir, "profiles.ndjson");
  const flatCsvFile = path.join(outputDir, "profiles_flat.csv");

  await ensureDir(outputDir);
  await ensureDir(profilesRoot);

  const csvText = await fsp.readFile(resolvedInputPath, "utf8");
  const parsed = buildProfileIndex(parseCsv(csvText));
  const selectedProfiles = applySelection(parsed.profiles, options.offset, options.limit);
  const cookieHeader = await getCookieHeader(options);
  const resumeState = await resolveResumeState({
    progressFile,
    inputCsv: resolvedInputPath,
    outputDir,
    profiles: selectedProfiles,
    profilesRoot,
    options,
  });
  let mongoSink = null;
  const stopController = createStopController();
  let vpnController = null;
  let proxyController = null;
  let rateController = null;

  try {
    vpnController = await createVpnController(options);
    proxyController = await createMullvadProxyController(options, vpnController);
    applyHighSpeedTuning(options, proxyController);
    rateController = createRateController(options, vpnController, proxyController);
    mongoSink = await createMongoSink(options);

    const runStartedAt = new Date().toISOString();
    const initialManifest = {
      started_at: runStartedAt,
      input_csv: resolvedInputPath,
      output_dir: outputDir,
      files: {
        profiles_root: profilesRoot,
        errors_ndjson: errorsFile,
        progress_json: progressFile,
        manifest_json: manifestFile,
        details_ndjson: detailsNdjsonFile,
        flat_csv: flatCsvFile,
      },
      options: sanitizeOptionsForManifest(options),
      source_rows: parsed.sourceRowCount,
      invalid_rows: parsed.invalidRows.length,
      unique_profiles_total: parsed.profiles.length,
      selected_profiles: selectedProfiles.length,
      duplicate_profile_rows: parsed.duplicateRowCount,
      mongo: mongoSink ? mongoSink.getSummary() : { enabled: false },
      throttle: rateController.getSummary(),
      vpn: vpnController ? vpnController.getSummary() : { enabled: false },
      proxy: proxyController ? proxyController.getSummary() : { enabled: false },
      resume: resumeState.summary,
    };

    await writeJsonAtomic(manifestFile, initialManifest);

    if (parsed.invalidRows.length > 0) {
      console.warn(
        `Skipping ${parsed.invalidRows.length} row(s) with missing or invalid Profile URL values.`
      );
    }

    let downloadSummary = {
      total: selectedProfiles.length,
      downloaded: 0,
      skipped_existing: 0,
      failed: 0,
      processed: 0,
      stopped_early: false,
    };

    if (!resumeState.enabled) {
      await fsp.writeFile(errorsFile, "", "utf8");
    } else {
      await appendNdjson(errorsFile, {
        type: "resume",
        resumed_at: new Date().toISOString(),
        start_index: resumeState.startIndex,
        checkpoint_status: resumeState.previous?.status || null,
      });
    }

    if (!options.summaryOnly) {
      downloadSummary = await downloadProfiles({
        profiles: selectedProfiles,
        profilesRoot,
        errorsFile,
        progressFile,
        options,
        cookieHeader,
        inputCsv: resolvedInputPath,
        outputDir,
        runStartedAt,
        mongoSink,
        rateController,
        vpnController,
        proxyController,
        stopController,
        startIndex: resumeState.startIndex,
      });
    } else if (mongoSink) {
      await syncLocalProfilesToMongo({
        profiles: selectedProfiles,
        profilesRoot,
        errorsFile,
        progressFile,
        runStartedAt,
        mongoSink,
      });
    }

    const runCompleted = downloadSummary.next_index >= downloadSummary.total;

    const aggregateSummary =
      !runCompleted || downloadSummary.stopped_by_signal || downloadSummary.stopped_early
        ? {
            skipped: true,
            reason: downloadSummary.stopped_by_signal
              ? "run_stopped_by_signal"
              : "run_incomplete",
          }
        : await buildAggregateOutputs({
            profiles: selectedProfiles,
            outputDir,
            profilesRoot,
            detailsNdjsonFile,
            flatCsvFile,
          });

    const finalManifest = {
      ...initialManifest,
      completed_at: new Date().toISOString(),
      download: downloadSummary,
      aggregate: aggregateSummary,
      mongo: mongoSink ? mongoSink.getSummary() : { enabled: false },
      throttle: rateController.getSummary(),
      vpn: vpnController ? vpnController.getSummary() : { enabled: false },
      proxy: proxyController ? proxyController.getSummary() : { enabled: false },
      invalid_rows_preview: parsed.invalidRows.slice(0, 25),
    };

    const finalStatus = downloadSummary.stopped_by_signal
      ? "stopped_by_signal"
      : !runCompleted
        ? "stopped_early"
        : downloadSummary.failed > 0
          ? "completed_with_errors"
          : "completed";

    await writeJsonAtomic(progressFile, {
      started_at: runStartedAt,
      completed_at: finalManifest.completed_at,
      status: finalStatus,
      ...downloadSummary,
      mongo: mongoSink ? mongoSink.getSummary() : null,
      throttle: rateController.getSummary(),
      vpn: vpnController ? vpnController.getSummary() : null,
      proxy: proxyController ? proxyController.getSummary() : null,
    });
    await writeJsonAtomic(manifestFile, finalManifest);

    console.log("");
    console.log("Run complete.");
    console.log(`Input CSV:     ${resolvedInputPath}`);
    console.log(`Output folder: ${outputDir}`);
    console.log(`Profiles:      ${downloadSummary.processed}/${downloadSummary.total} processed`);
    console.log(`Downloaded:    ${downloadSummary.downloaded}`);
    console.log(`Skipped:       ${downloadSummary.skipped_existing}`);
    console.log(`Failed:        ${downloadSummary.failed}`);
    if (mongoSink) {
      const mongoSummary = mongoSink.getSummary();
      console.log(
        `MongoDB:       ${mongoSummary.inserted} inserted, ${mongoSummary.updated} updated`
      );
      console.log(
        `Mongo target:  ${mongoSummary.database}.${mongoSummary.collection} @ ${mongoSummary.uri}`
      );
    }
    const throttleSummary = rateController.getSummary();
    console.log(
      `Throttle:      gap=${throttleSummary.current_request_gap_ms}ms, rate_limits=${throttleSummary.rate_limit_hits}`
    );
    if (vpnController) {
      const vpnSummary = vpnController.getSummary();
      console.log(
        `VPN:           rotations=${vpnSummary.rotations_completed}, current_ip=${vpnSummary.current_ip || "unknown"}`
      );
    }
    if (proxyController) {
      const proxySummary = proxyController.getSummary();
      console.log(
        `Proxy:         ${proxySummary.current_proxy_label || "none"} | remote_rotations=${proxySummary.remote_rotations_completed}`
      );
    }
    console.log(`NDJSON:        ${detailsNdjsonFile}`);
    console.log(`Flat CSV:      ${flatCsvFile}`);

    if (downloadSummary.stopped_by_signal) {
      process.exitCode = 130;
    } else if (downloadSummary.failed > 0) {
      process.exitCode = 2;
    }
  } finally {
    stopController.dispose();
    if (mongoSink) {
      await mongoSink.close();
    }
  }
}

function parseArgs(argv) {
  const options = {
    concurrency: DEFAULT_CONCURRENCY,
    timeoutMs: DEFAULT_TIMEOUT_MS,
    retries: DEFAULT_RETRIES,
    retryBaseMs: DEFAULT_RETRY_BASE_MS,
    logEvery: DEFAULT_LOG_EVERY,
    maxErrors: DEFAULT_MAX_ERRORS,
    minRequestGapMs: DEFAULT_MIN_REQUEST_GAP_MS,
    maxRequestGapMs: DEFAULT_MAX_REQUEST_GAP_MS,
    rateLimitCooldownMs: DEFAULT_RATE_LIMIT_COOLDOWN_MS,
    maxRateLimitCooldownMs: DEFAULT_MAX_RATE_LIMIT_COOLDOWN_MS,
    transientErrorCooldownMs: DEFAULT_TRANSIENT_ERROR_COOLDOWN_MS,
    resume: true,
    resumeLookback: DEFAULT_RESUME_LOOKBACK,
    mullvadRotateOnRateLimit: DEFAULT_MULLVAD_ROTATE_ON_RATE_LIMIT,
    mullvadCommand: DEFAULT_MULLVAD_COMMAND,
    mullvadReconnectAttempts: DEFAULT_MULLVAD_RECONNECT_ATTEMPTS,
    mullvadReconnectTimeoutMs: DEFAULT_MULLVAD_RECONNECT_TIMEOUT_MS,
    mullvadSettleMs: DEFAULT_MULLVAD_SETTLE_MS,
    mullvadProxyMode: DEFAULT_MULLVAD_PROXY_MODE,
    mullvadProxyHost: DEFAULT_MULLVAD_PROXY_HOST,
    mullvadProxyPort: DEFAULT_MULLVAD_PROXY_PORT,
    mullvadProxyCountry: DEFAULT_MULLVAD_PROXY_COUNTRY,
    mullvadProxyMaxRelays: DEFAULT_MULLVAD_PROXY_MAX_RELAYS,
    mullvadProxyMaxInFlight: DEFAULT_MULLVAD_PROXY_MAX_INFLIGHT,
    highSpeedMode: DEFAULT_HIGH_SPEED_MODE,
    mongoUri: DEFAULT_MONGO_URI,
    mongoDb: DEFAULT_MONGO_DB,
    mongoCollection: DEFAULT_MONGO_COLLECTION,
    mongoTimeoutMs: DEFAULT_MONGO_TIMEOUT_MS,
    mongoEnabled: true,
    offset: 0,
    limit: null,
    force: false,
    summaryOnly: false,
    help: false,
    cookie: process.env.BRAINTRUST_COOKIE || "",
    cookieFile: "",
    concurrencyExplicit: false,
    minRequestGapExplicit: false,
    retriesExplicit: false,
    logEveryExplicit: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];

    if (arg === "--help" || arg === "-h") {
      options.help = true;
      continue;
    }

    if (arg === "--force") {
      options.force = true;
      continue;
    }

    if (arg === "--summary-only") {
      options.summaryOnly = true;
      continue;
    }

    if (arg === "--no-mongo") {
      options.mongoEnabled = false;
      continue;
    }

    if (arg === "--no-resume") {
      options.resume = false;
      continue;
    }

    if (arg === "--no-mullvad-rotate") {
      options.mullvadRotateOnRateLimit = false;
      continue;
    }

    if (arg === "--no-mullvad-proxy") {
      options.mullvadProxyMode = false;
      continue;
    }

    if (arg === "--high-speed") {
      options.highSpeedMode = true;
      continue;
    }

    if (!arg.startsWith("--")) {
      throw new Error(`Unexpected argument: ${arg}`);
    }

    const key = arg.replace(/^--/, "");
    const nextValue = argv[index + 1];
    if (nextValue == null) {
      throw new Error(`Missing value for --${key}`);
    }
    index += 1;

    switch (key) {
      case "input":
        options.input = nextValue;
        break;
      case "out":
        options.out = nextValue;
        break;
      case "concurrency":
        options.concurrency = toPositiveInteger(nextValue, "--concurrency");
        options.concurrencyExplicit = true;
        break;
      case "timeout-ms":
        options.timeoutMs = toPositiveInteger(nextValue, "--timeout-ms");
        break;
      case "retries":
        options.retries = toNonNegativeInteger(nextValue, "--retries");
        options.retriesExplicit = true;
        break;
      case "retry-base-ms":
        options.retryBaseMs = toPositiveInteger(nextValue, "--retry-base-ms");
        break;
      case "log-every":
        options.logEvery = toPositiveInteger(nextValue, "--log-every");
        options.logEveryExplicit = true;
        break;
      case "max-errors":
        options.maxErrors = toPositiveInteger(nextValue, "--max-errors");
        break;
      case "min-request-gap-ms":
        options.minRequestGapMs = toNonNegativeInteger(nextValue, "--min-request-gap-ms");
        options.minRequestGapExplicit = true;
        break;
      case "max-request-gap-ms":
        options.maxRequestGapMs = toPositiveInteger(nextValue, "--max-request-gap-ms");
        break;
      case "rate-limit-cooldown-ms":
        options.rateLimitCooldownMs = toPositiveInteger(nextValue, "--rate-limit-cooldown-ms");
        break;
      case "max-rate-limit-cooldown-ms":
        options.maxRateLimitCooldownMs = toPositiveInteger(
          nextValue,
          "--max-rate-limit-cooldown-ms"
        );
        break;
      case "transient-error-cooldown-ms":
        options.transientErrorCooldownMs = toPositiveInteger(
          nextValue,
          "--transient-error-cooldown-ms"
        );
        break;
      case "resume-lookback":
        options.resumeLookback = toNonNegativeInteger(nextValue, "--resume-lookback");
        break;
      case "mullvad-command":
        options.mullvadCommand = nextValue;
        break;
      case "mullvad-reconnect-attempts":
        options.mullvadReconnectAttempts = toPositiveInteger(
          nextValue,
          "--mullvad-reconnect-attempts"
        );
        break;
      case "mullvad-reconnect-timeout-ms":
        options.mullvadReconnectTimeoutMs = toPositiveInteger(
          nextValue,
          "--mullvad-reconnect-timeout-ms"
        );
        break;
      case "mullvad-settle-ms":
        options.mullvadSettleMs = toNonNegativeInteger(nextValue, "--mullvad-settle-ms");
        break;
      case "mullvad-proxy-host":
        options.mullvadProxyHost = nextValue;
        break;
      case "mullvad-proxy-port":
        options.mullvadProxyPort = toPositiveInteger(nextValue, "--mullvad-proxy-port");
        break;
      case "mullvad-proxy-country":
        options.mullvadProxyCountry = nextValue.toLowerCase();
        break;
      case "mullvad-proxy-max-relays":
        options.mullvadProxyMaxRelays = toPositiveInteger(nextValue, "--mullvad-proxy-max-relays");
        break;
      case "mullvad-proxy-max-inflight":
        options.mullvadProxyMaxInFlight = toPositiveInteger(
          nextValue,
          "--mullvad-proxy-max-inflight"
        );
        break;
      case "mongo-uri":
        options.mongoUri = nextValue;
        break;
      case "mongo-db":
        options.mongoDb = nextValue;
        break;
      case "mongo-collection":
        options.mongoCollection = nextValue;
        break;
      case "mongo-timeout-ms":
        options.mongoTimeoutMs = toPositiveInteger(nextValue, "--mongo-timeout-ms");
        break;
      case "offset":
        options.offset = toNonNegativeInteger(nextValue, "--offset");
        break;
      case "limit":
        options.limit = toPositiveInteger(nextValue, "--limit");
        break;
      case "cookie":
        options.cookie = nextValue;
        break;
      case "cookie-file":
        options.cookieFile = nextValue;
        break;
      default:
        throw new Error(`Unknown option: --${key}`);
    }
  }

  if (options.maxRequestGapMs < options.minRequestGapMs) {
    throw new Error("--max-request-gap-ms must be greater than or equal to --min-request-gap-ms.");
  }

  if (options.maxRateLimitCooldownMs < options.rateLimitCooldownMs) {
    throw new Error(
      "--max-rate-limit-cooldown-ms must be greater than or equal to --rate-limit-cooldown-ms."
    );
  }

  if (
    !["auto", "any"].includes(options.mullvadProxyCountry) &&
    !/^[a-z]{2}$/.test(options.mullvadProxyCountry)
  ) {
    throw new Error("--mullvad-proxy-country must be auto, any, or a two-letter country code.");
  }

  return options;
}

function printHelp() {
  console.log(`
Usage:
  node download_braintrust_profiles.js [options]

Options:
  --input <file>          Input CSV file. Defaults to the first *_filtered.csv in the cwd.
  --out <dir>             Output folder. Default: ./braintrust_profile_details
  --concurrency <n>       Parallel requests. Default: ${DEFAULT_CONCURRENCY}
  --timeout-ms <n>        Per-request timeout in milliseconds. Default: ${DEFAULT_TIMEOUT_MS}
  --retries <n>           Retries for 429/5xx/network errors. Default: ${DEFAULT_RETRIES}
  --retry-base-ms <n>     Base backoff in milliseconds. Default: ${DEFAULT_RETRY_BASE_MS}
  --log-every <n>         Progress log cadence. Default: ${DEFAULT_LOG_EVERY}
  --max-errors <n>        Stop after this many failed profiles. Default: ${DEFAULT_MAX_ERRORS}
  --min-request-gap-ms    Minimum delay between request starts. Default: ${DEFAULT_MIN_REQUEST_GAP_MS}
  --max-request-gap-ms    Maximum adaptive delay between request starts. Default: ${DEFAULT_MAX_REQUEST_GAP_MS}
  --rate-limit-cooldown-ms  Shared pause after 429/rate-limit. Default: ${DEFAULT_RATE_LIMIT_COOLDOWN_MS}
  --max-rate-limit-cooldown-ms  Max shared pause after repeated 429s. Default: ${DEFAULT_MAX_RATE_LIMIT_COOLDOWN_MS}
  --transient-error-cooldown-ms  Shared pause after network/5xx issues. Default: ${DEFAULT_TRANSIENT_ERROR_COOLDOWN_MS}
  --no-resume              Ignore saved checkpoint and start from the beginning.
  --resume-lookback <n>    Recheck the last N indices before saved position. Default: ${DEFAULT_RESUME_LOOKBACK}
  --high-speed             Auto-tune for maximum proxy throughput while isolating rate-limited exits.
  --no-mullvad-proxy       Disable Mullvad SOCKS5 proxy mode.
  --mullvad-proxy-host <host>  Local Mullvad SOCKS5 host. Default: ${DEFAULT_MULLVAD_PROXY_HOST}
  --mullvad-proxy-port <n> Local/remote Mullvad SOCKS5 port. Default: ${DEFAULT_MULLVAD_PROXY_PORT}
  --mullvad-proxy-country <code|auto|any>  Remote SOCKS5 relay country selection. Default: ${DEFAULT_MULLVAD_PROXY_COUNTRY}
  --mullvad-proxy-max-relays <n>  Max remote SOCKS5 relays to keep in rotation. Default: ${DEFAULT_MULLVAD_PROXY_MAX_RELAYS}
  --mullvad-proxy-max-inflight <n>  Max concurrent requests per proxy exit. Default: ${DEFAULT_MULLVAD_PROXY_MAX_INFLIGHT}
  --no-mullvad-rotate      Disable Mullvad IP rotation on rate limits.
  --mullvad-command <cmd>  Mullvad CLI command. Default: ${DEFAULT_MULLVAD_COMMAND}
  --mullvad-reconnect-attempts <n>  Mullvad reconnect attempts per rotation. Default: ${DEFAULT_MULLVAD_RECONNECT_ATTEMPTS}
  --mullvad-reconnect-timeout-ms <n>  Timeout for each Mullvad reconnect. Default: ${DEFAULT_MULLVAD_RECONNECT_TIMEOUT_MS}
  --mullvad-settle-ms <n>  Wait after reconnect before resuming. Default: ${DEFAULT_MULLVAD_SETTLE_MS}
  --mongo-uri <uri>       MongoDB connection string. Default: ${DEFAULT_MONGO_URI}
  --mongo-db <name>       Mongo database name. Default: ${DEFAULT_MONGO_DB}
  --mongo-collection <n>  Mongo collection name. Default: ${DEFAULT_MONGO_COLLECTION}
  --mongo-timeout-ms <n>  Mongo connect timeout. Default: ${DEFAULT_MONGO_TIMEOUT_MS}
  --no-mongo              Disable MongoDB writes.
  --offset <n>            Skip the first N unique profile IDs before downloading.
  --limit <n>             Only process N unique profile IDs.
  --force                 Re-download profiles even if the JSON file already exists.
  --summary-only          Skip downloading and rebuild profiles.ndjson / profiles_flat.csv.
  --cookie <value>        Optional Cookie header value.
  --cookie-file <file>    Optional text file containing the Cookie header value.
  --help                  Show this help.

Examples:
  node download_braintrust_profiles.js --input braintrust_developers_2026-04-01T17-58-21-624Z_filtered.csv
  node download_braintrust_profiles.js --concurrency 6 --limit 500
  node download_braintrust_profiles.js --concurrency 1 --min-request-gap-ms 1500
  node download_braintrust_profiles.js --high-speed --mullvad-proxy-country any --mullvad-proxy-max-relays 48
  node download_braintrust_profiles.js --mullvad-proxy-country us --mullvad-proxy-max-relays 24
  node download_braintrust_profiles.js --resume-lookback 50
  node download_braintrust_profiles.js --mongo-uri mongodb://localhost:27017/ --mongo-db braintrust --mongo-collection profiles
  node download_braintrust_profiles.js --out ./run_01 --force
  node download_braintrust_profiles.js --summary-only --out ./braintrust_profile_details
  `);
}

function applyHighSpeedTuning(options, proxyController) {
  if (!options.highSpeedMode || !proxyController) {
    return;
  }

  const proxySummary = proxyController.getSummary();
  const candidateCount = Math.max(1, Number(proxySummary?.candidate_count) || 1);

  if (!options.concurrencyExplicit) {
    options.concurrency = clampNumber(
      candidateCount * Math.max(1, options.mullvadProxyMaxInFlight),
      8,
      32
    );
  }

  if (!options.minRequestGapExplicit) {
    options.minRequestGapMs = 0;
  }

  if (!options.retriesExplicit) {
    options.retries = Math.max(options.retries, 8);
  }

  if (!options.logEveryExplicit) {
    options.logEvery = Math.max(options.logEvery, 100);
  }
}

function detectDefaultInput() {
  const cwdEntries = fs.readdirSync(process.cwd(), { withFileTypes: true });
  const csvCandidates = cwdEntries
    .filter((entry) => entry.isFile())
    .map((entry) => entry.name)
    .filter((name) => /filtered\.csv$/i.test(name))
    .sort();

  return csvCandidates.length > 0 ? csvCandidates[csvCandidates.length - 1] : null;
}

function toPositiveInteger(value, label) {
  const parsed = Number.parseInt(String(value), 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${label} must be a positive integer.`);
  }
  return parsed;
}

function toNonNegativeInteger(value, label) {
  const parsed = Number.parseInt(String(value), 10);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`${label} must be a non-negative integer.`);
  }
  return parsed;
}

function parseCsv(text) {
  const rows = [];
  const normalized = text.charCodeAt(0) === 0xfeff ? text.slice(1) : text;
  let field = "";
  let row = [];
  let inQuotes = false;

  for (let index = 0; index < normalized.length; index += 1) {
    const char = normalized[index];

    if (inQuotes) {
      if (char === '"') {
        if (normalized[index + 1] === '"') {
          field += '"';
          index += 1;
        } else {
          inQuotes = false;
        }
      } else {
        field += char;
      }
      continue;
    }

    if (char === '"') {
      inQuotes = true;
      continue;
    }

    if (char === ",") {
      row.push(field);
      field = "";
      continue;
    }

    if (char === "\n") {
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
      continue;
    }

    if (char === "\r") {
      continue;
    }

    field += char;
  }

  if (field.length > 0 || row.length > 0) {
    row.push(field);
    rows.push(row);
  }

  return rows.filter((currentRow) => currentRow.some((cell) => cell !== ""));
}

function buildProfileIndex(rows) {
  if (rows.length < 2) {
    throw new Error("Input CSV does not contain any data rows.");
  }

  const header = rows[0];
  const profilesById = new Map();
  const invalidRows = [];
  let duplicateRowCount = 0;

  for (let rowIndex = 1; rowIndex < rows.length; rowIndex += 1) {
    const row = rows[rowIndex];
    const record = Object.fromEntries(
      header.map((columnName, columnIndex) => [columnName, row[columnIndex] ?? ""])
    );

    const profileUrl = record["Profile URL"] || "";
    const profileId = extractProfileId(profileUrl);

    if (!profileId) {
      invalidRows.push({
        row_number: rowIndex + 1,
        state: record.State || "",
        user_id: record["User ID"] || "",
        first_name: record["First Name"] || "",
        last_name: record["Last Name"] || "",
        profile_url: profileUrl,
      });
      continue;
    }

    const source = {
      row_number: rowIndex + 1,
      state: record.State || "",
      user_id: record["User ID"] || "",
      first_name: record["First Name"] || "",
      last_name: record["Last Name"] || "",
      profile_url: profileUrl,
      profile_id: profileId,
      profile_page_url: toAbsoluteProfileUrl(profileUrl),
      profile_api_url: buildApiUrl(profileId),
    };

    let profileEntry = profilesById.get(profileId);
    if (!profileEntry) {
      profileEntry = {
        profile_id: profileId,
        profile_page_url: source.profile_page_url,
        profile_api_url: source.profile_api_url,
        sources: [],
      };
      profilesById.set(profileId, profileEntry);
    } else {
      duplicateRowCount += 1;
    }

    profileEntry.sources.push(source);
  }

  const profiles = Array.from(profilesById.values());

  return {
    profiles,
    sourceRowCount: rows.length - 1,
    invalidRows,
    duplicateRowCount,
  };
}

function applySelection(profiles, offset, limit) {
  const start = Math.min(offset || 0, profiles.length);
  const end =
    limit == null ? profiles.length : Math.min(profiles.length, start + Math.max(0, limit));
  return profiles.slice(start, end);
}

function extractProfileId(profileUrl) {
  if (!profileUrl) {
    return null;
  }

  const text = String(profileUrl).trim();
  const match =
    text.match(/\/talent\/(\d+)\/?/i) ||
    text.match(/\/freelancers\/(\d+)\/?/i) ||
    text.match(/\/api\/freelancers\/(\d+)\/?/i);

  return match ? match[1] : null;
}

function toAbsoluteProfileUrl(profileUrl) {
  const value = String(profileUrl || "").trim();
  if (!value) {
    return "";
  }

  try {
    return new URL(value, BASE_URL).href;
  } catch (error) {
    return value;
  }
}

function buildApiUrl(profileId) {
  return new URL(`${API_PREFIX}${profileId}/`, BASE_URL).href;
}

async function getCookieHeader(options) {
  if (options.cookieFile) {
    const cookieText = await fsp.readFile(path.resolve(options.cookieFile), "utf8");
    return cookieText.trim();
  }

  return String(options.cookie || "").trim();
}

async function downloadProfiles(params) {
  const {
    profiles,
    profilesRoot,
    errorsFile,
    progressFile,
    options,
    cookieHeader,
    inputCsv,
    outputDir,
    runStartedAt,
    mongoSink,
    rateController,
    vpnController,
    proxyController,
    stopController,
    startIndex,
  } = params;

  let nextIndex = startIndex;
  let stopRequested = false;
  let lastLoggedAt = 0;

  const summary = {
    total: profiles.length,
    start_index: startIndex,
    downloaded: 0,
    skipped_existing: 0,
    failed: 0,
    processed: 0,
    stopped_early: false,
    stopped_by_signal: false,
    stop_signal: null,
    next_index: nextIndex,
  };

  await writeJsonAtomic(progressFile, {
    started_at: runStartedAt,
    status: "running",
    input_csv: inputCsv,
    output_dir: outputDir,
    ...summary,
    mongo: mongoSink ? mongoSink.getSummary() : null,
    throttle: rateController.getSummary(),
    vpn: vpnController ? vpnController.getSummary() : null,
    proxy: proxyController ? proxyController.getSummary() : null,
  });

  const workers = Array.from({ length: options.concurrency }, (_, workerIndex) =>
    runWorker(workerIndex + 1)
  );
  await Promise.all(workers);
  summary.next_index = Math.min(nextIndex, profiles.length);
  summary.stopped_by_signal = Boolean(stopController.signal);
  summary.stop_signal = stopController.signal;

  async function runWorker(workerId) {
    while (!stopRequested) {
      if (stopController.requested) {
        stopRequested = true;
        summary.stopped_early = true;
        summary.stopped_by_signal = true;
        summary.stop_signal = stopController.signal;
        summary.next_index = Math.min(nextIndex, profiles.length);
        return;
      }

      if (nextIndex >= profiles.length) {
        summary.next_index = profiles.length;
        return;
      }

      const currentIndex = nextIndex;
      nextIndex += 1;
      summary.next_index = Math.min(nextIndex, profiles.length);

      const profileEntry = profiles[currentIndex];
      const profileFile = getProfileFilePath(profilesRoot, profileEntry.profile_id);

      try {
        if (!options.force && (await fileExistsWithContent(profileFile))) {
          const existingWrapper = JSON.parse(await fsp.readFile(profileFile, "utf8"));
          if (mongoSink) {
            await mongoSink.upsertWrapper(existingWrapper, "cache");
          }
          summary.skipped_existing += 1;
          await reportProgress(`worker ${workerId} skipped ${profileEntry.profile_id}`);
          continue;
        }

        const result = await fetchProfileWithRetries({
          profileId: profileEntry.profile_id,
          apiUrl: profileEntry.profile_api_url,
          timeoutMs: options.timeoutMs,
          retries: options.retries,
          retryBaseMs: options.retryBaseMs,
          cookieHeader,
          rateController,
          proxyController,
        });

        const wrapper = {
          profile_id: profileEntry.profile_id,
          profile_page_url: profileEntry.profile_page_url,
          profile_api_url: profileEntry.profile_api_url,
          fetched_at: result.fetchedAt,
          http: {
            status: result.status,
            attempts: result.attempts,
            duration_ms: result.durationMs,
            etag: result.etag,
            last_modified: result.lastModified,
            content_type: result.contentType,
            proxy_label: result.proxyLabel,
            proxy_uri: result.proxyUri,
          },
          sources: profileEntry.sources,
          profile: result.data,
        };

        await writeJsonAtomic(profileFile, wrapper);
        if (mongoSink) {
          await mongoSink.upsertWrapper(wrapper, "download");
        }
        summary.downloaded += 1;
      } catch (error) {
        summary.failed += 1;
        const errorRecord = {
          profile_id: profileEntry.profile_id,
          profile_page_url: profileEntry.profile_page_url,
          profile_api_url: profileEntry.profile_api_url,
          failed_at: new Date().toISOString(),
          error: serializeError(error),
          sources: profileEntry.sources,
        };
        await appendNdjson(errorsFile, errorRecord);

        console.error(
          `[error] profile ${profileEntry.profile_id} failed: ${errorRecord.error.message}`
        );

        if (summary.failed >= options.maxErrors) {
          stopRequested = true;
          summary.stopped_early = true;
        }
      } finally {
        summary.processed += 1;
        await reportProgress(`worker ${workerId} processed ${profileEntry.profile_id}`);
      }
    }
  }

  async function reportProgress(reason) {
    const now = Date.now();
    const shouldLog =
      summary.processed === summary.total ||
      summary.processed % options.logEvery === 0 ||
      now - lastLoggedAt >= 15000;

    if (!shouldLog) {
      return;
    }

    lastLoggedAt = now;

    const payload = {
      started_at: runStartedAt,
      updated_at: new Date().toISOString(),
      status: stopRequested ? "stopping" : "running",
      reason,
      ...summary,
      mongo: mongoSink ? mongoSink.getSummary() : null,
      throttle: rateController.getSummary(),
      vpn: vpnController ? vpnController.getSummary() : null,
      proxy: proxyController ? proxyController.getSummary() : null,
    };

    await writeJsonAtomic(progressFile, payload);
    const throttle = payload.throttle || {};
    const proxy = payload.proxy || {};
    console.log(
      `[progress] ${summary.processed}/${summary.total} processed | downloaded=${summary.downloaded} skipped=${summary.skipped_existing} failed=${summary.failed} | gap=${throttle.current_request_gap_ms || 0}ms pause=${throttle.pause_remaining_ms || 0}ms rateLimits=${throttle.rate_limit_hits || 0} | proxy=${proxy.current_proxy_label || "none"} inflight=${proxy.inflight_total || 0}/${proxy.candidate_count || 0}`
    );
  }

  return summary;
}

async function syncLocalProfilesToMongo(params) {
  const { profiles, profilesRoot, errorsFile, progressFile, runStartedAt, mongoSink } = params;

  let processed = 0;
  let failed = 0;
  let lastLoggedAt = 0;

  for (const entry of profiles) {
    const profileFile = getProfileFilePath(profilesRoot, entry.profile_id);

    try {
      if (await fileExistsWithContent(profileFile)) {
        const wrapper = JSON.parse(await fsp.readFile(profileFile, "utf8"));
        await mongoSink.upsertWrapper(wrapper, "cache");
      }
    } catch (error) {
      failed += 1;
      await appendNdjson(errorsFile, {
        profile_id: entry.profile_id,
        profile_api_url: entry.profile_api_url,
        failed_at: new Date().toISOString(),
        error: serializeError(error),
        sources: entry.sources,
      });
    }

    processed += 1;

    const now = Date.now();
    if (processed === profiles.length || now - lastLoggedAt >= 15000 || processed % 100 === 0) {
      lastLoggedAt = now;
      await writeJsonAtomic(progressFile, {
        started_at: runStartedAt,
        updated_at: new Date().toISOString(),
        status: "syncing_mongo_from_files",
        processed,
        total: profiles.length,
        failed,
        mongo: mongoSink.getSummary(),
      });
    }
  }
}

async function createMongoSink(options) {
  if (!options.mongoEnabled) {
    return null;
  }

  if (!MongoClient) {
    throw new Error(
      "MongoDB support requires the `mongodb` package. Run `npm install` in this folder."
    );
  }

  const client = new MongoClient(options.mongoUri, {
    serverSelectionTimeoutMS: options.mongoTimeoutMs,
  });

  try {
    await client.connect();
    const database = client.db(options.mongoDb);
    const collection = database.collection(options.mongoCollection);
    await collection.createIndex({ profile_id: 1 }, { unique: true });

    const summary = {
      enabled: true,
      uri: redactMongoUri(options.mongoUri),
      database: options.mongoDb,
      collection: options.mongoCollection,
      inserted: 0,
      updated: 0,
      errors: 0,
      writes_from_download: 0,
      writes_from_cache: 0,
    };

    return {
      async upsertWrapper(wrapper, sourceKind) {
        try {
          const document = {
            ...wrapper,
            mongo_saved_at: new Date().toISOString(),
          };
          const result = await collection.replaceOne(
            { profile_id: document.profile_id },
            document,
            { upsert: true }
          );
          if (result.upsertedCount > 0) {
            summary.inserted += 1;
          } else {
            summary.updated += 1;
          }

          if (sourceKind === "download") {
            summary.writes_from_download += 1;
          } else {
            summary.writes_from_cache += 1;
          }
        } catch (error) {
          summary.errors += 1;
          throw error;
        }
      },
      getSummary() {
        return { ...summary };
      },
      async close() {
        await client.close();
      },
    };
  } catch (error) {
    await client.close().catch(() => {});
    throw new Error(
      `Unable to connect to MongoDB at ${redactMongoUri(options.mongoUri)}: ${error.message}`
    );
  }
}

function createStopController() {
  let requested = false;
  let signal = null;

  const handleSignal = (name) => {
    if (requested) {
      console.error(`[signal] ${name} received again. Exiting immediately.`);
      process.exit(130);
    }

    requested = true;
    signal = name;
    console.error(`[signal] ${name} received. Finishing in-flight work and stopping...`);
  };

  const onSigint = () => handleSignal("SIGINT");
  const onSigterm = () => handleSignal("SIGTERM");
  process.on("SIGINT", onSigint);
  process.on("SIGTERM", onSigterm);

  return {
    get requested() {
      return requested;
    },
    get signal() {
      return signal;
    },
    dispose() {
      process.off("SIGINT", onSigint);
      process.off("SIGTERM", onSigterm);
    },
  };
}

async function resolveResumeState(params) {
  const { progressFile, inputCsv, outputDir, profiles, profilesRoot, options } = params;
  const base = {
    enabled: false,
    startIndex: 0,
    previous: null,
    summary: {
      enabled: false,
      reason: "disabled_or_unavailable",
      start_index: 0,
      checkpoint_next_index: 0,
      lookback: options.resumeLookback,
    },
  };

  if (!options.resume || options.force) {
    return base;
  }

  const previous = await readJsonIfExists(progressFile);
  if (!previous) {
    return base;
  }

  const sameInput = path.resolve(previous.input_csv || "") === inputCsv;
  const sameOutput = path.resolve(previous.output_dir || "") === outputDir;
  const status = String(previous.status || "");

  if (!sameInput || !sameOutput) {
    return {
      ...base,
      previous,
      summary: {
        enabled: false,
        reason: "checkpoint_mismatch",
        start_index: 0,
        checkpoint_next_index: 0,
        lookback: options.resumeLookback,
      },
    };
  }

  if (status === "completed" || status === "completed_with_errors") {
    return {
      ...base,
      previous,
      summary: {
        enabled: false,
        reason: "already_completed",
        start_index: 0,
        checkpoint_next_index: previous.next_index || 0,
        lookback: options.resumeLookback,
      },
    };
  }

  const checkpointNextIndex = clampNumber(
    previous.next_index ?? previous.processed ?? 0,
    0,
    profiles.length
  );
  const windowStart = Math.max(0, checkpointNextIndex - options.resumeLookback);
  let startIndex = checkpointNextIndex;

  for (let index = windowStart; index < checkpointNextIndex; index += 1) {
    const profileFile = getProfileFilePath(profilesRoot, profiles[index].profile_id);
    if (!(await fileExistsWithContent(profileFile))) {
      startIndex = index;
      break;
    }
  }

  return {
    enabled: true,
    startIndex,
    previous,
    summary: {
      enabled: true,
      reason: "checkpoint_found",
      start_index: startIndex,
      checkpoint_next_index: checkpointNextIndex,
      lookback: options.resumeLookback,
      previous_status: status,
    },
  };
}

async function createVpnController(options) {
  if (!options.mullvadRotateOnRateLimit) {
    return null;
  }

  const summary = {
    enabled: true,
    command: options.mullvadCommand,
    reconnect_attempts: options.mullvadReconnectAttempts,
    reconnect_timeout_ms: options.mullvadReconnectTimeoutMs,
    settle_ms: options.mullvadSettleMs,
    rotations_started: 0,
    rotations_completed: 0,
    rotations_failed: 0,
    current_relay: null,
    current_ip: null,
    last_before: null,
    last_after: null,
    last_reason: null,
    last_error: null,
    last_rotated_at: null,
  };

  await runCommand(options.mullvadCommand, ["version"], options.mullvadReconnectTimeoutMs);
  const currentStatus = await getMullvadStatus(options).catch(() => null);
  if (currentStatus) {
    summary.current_relay = currentStatus.relay;
    summary.current_ip = currentStatus.ipv4;
  }

  let rotationPromise = null;

  return {
    async waitForReady() {
      if (rotationPromise) {
        await rotationPromise;
      }
    },

    async rotate(context) {
      if (rotationPromise) {
        return rotationPromise;
      }

      rotationPromise = (async () => {
        summary.rotations_started += 1;
        summary.last_reason = context || null;
        summary.last_error = null;

        const before = await getMullvadStatus(options).catch(() => null);
        summary.last_before = before;

        let finalStatus = before;
        for (let attempt = 1; attempt <= options.mullvadReconnectAttempts; attempt += 1) {
          await runCommand(
            options.mullvadCommand,
            ["reconnect", "--wait"],
            options.mullvadReconnectTimeoutMs
          );

          if (options.mullvadSettleMs > 0) {
            await sleep(options.mullvadSettleMs);
          }

          finalStatus = await getMullvadStatus(options);
          if (isMullvadStatusDifferent(before, finalStatus)) {
            summary.rotations_completed += 1;
            summary.current_relay = finalStatus.relay;
            summary.current_ip = finalStatus.ipv4;
            summary.last_after = finalStatus;
            summary.last_rotated_at = new Date().toISOString();
            return finalStatus;
          }
        }

        summary.rotations_failed += 1;
        summary.last_after = finalStatus;
        throw new Error(
          `Mullvad reconnect did not produce a new IP/relay after ${options.mullvadReconnectAttempts} attempts.`
        );
      })()
        .catch((error) => {
          summary.last_error = error?.message || String(error);
          throw error;
        })
        .finally(() => {
          rotationPromise = null;
        });

      return rotationPromise;
    },

    getSummary() {
      return {
        ...summary,
        rotating: Boolean(rotationPromise),
      };
    },
  };
}

async function createMullvadProxyController(options, vpnController) {
  if (!options.mullvadProxyMode) {
    return null;
  }

  await runCommand(options.mullvadCommand, ["version"], options.mullvadReconnectTimeoutMs);

  const status = vpnController
    ? await getMullvadStatus(options)
    : await getMullvadStatus(options).catch(() => null);
  const SocksProxyAgent = await getSocksProxyAgentClass();

  if (!status || !status.connected) {
    throw new Error(
      "Mullvad SOCKS5 proxy mode requires Mullvad to be connected. Connect Mullvad first or use --no-mullvad-proxy."
    );
  }

  const countryFilter = resolveMullvadProxyCountry(options, status);
  const relayCodes = await getMullvadWireGuardRelayCodes(options, countryFilter);
  const remoteRelayCodes = shuffleArray(
    relayCodes.filter((code) => code !== status.relay_code)
  ).slice(0, options.mullvadProxyMaxRelays);

  const candidates = [
    createProxyCandidate({
      key: "local",
      label: `local:${options.mullvadProxyHost}:${options.mullvadProxyPort}`,
      uri: buildSocksProxyUri(options.mullvadProxyHost, options.mullvadProxyPort),
      remote: false,
      relayCode: status.relay_code,
      SocksProxyAgent,
    }),
    ...remoteRelayCodes.map((relayCode) =>
      createProxyCandidate({
        key: `remote:${relayCode}`,
        label: relayCode,
        uri: buildSocksProxyUri(toMullvadSocksHostname(relayCode), options.mullvadProxyPort),
        remote: true,
        relayCode,
        SocksProxyAgent,
      })
    ),
  ];

  let nextCandidateIndex = 0;

  const summary = {
    enabled: true,
    country_filter: countryFilter,
    candidate_count: candidates.length,
    remote_candidate_count: remoteRelayCodes.length,
    max_inflight_per_proxy: options.mullvadProxyMaxInFlight,
    inflight_total: 0,
    current_proxy_label: candidates[0]?.label || null,
    current_proxy_uri: candidates[0]?.uri || null,
    remote_rotations_completed: 0,
    rate_limit_bans: 0,
    last_rate_limited_proxy: null,
    last_proxy_error: null,
    last_event: null,
  };

  return {
    async waitForReady() {
      return;
    },

    async acquire(label) {
      const candidate = await selectAvailableProxy();
      candidate.inFlight += 1;
      candidate.totalLeases += 1;
      candidate.lastUsedAt = new Date().toISOString();
      summary.inflight_total = getInFlightTotal();
      summary.current_proxy_label = candidate.label;
      summary.current_proxy_uri = candidate.uri;
      summary.last_event = {
        type: "acquire_proxy",
        at: candidate.lastUsedAt,
        label,
        proxy: candidate.label,
      };
      return candidate;
    },

    release(candidate) {
      if (!candidate) {
        return;
      }
      candidate.inFlight = Math.max(0, candidate.inFlight - 1);
      summary.inflight_total = getInFlightTotal();
    },

    noteSuccess(candidate) {
      if (!candidate) {
        return;
      }
      candidate.successes += 1;
      candidate.lastSucceededAt = new Date().toISOString();
    },

    async handleRateLimit(error, meta) {
      const candidate = candidates.find((item) => item.key === meta?.proxyKey);
      if (!candidate) {
        return candidates.length > 1;
      }

      const banMs = Math.max(options.rateLimitCooldownMs, error?.retryAfterMs || 0);
      candidate.cooldownUntil = Date.now() + banMs;
      candidate.rateLimitHits += 1;
      summary.rate_limit_bans += 1;
      summary.last_rate_limited_proxy = candidate.label;
      summary.last_event = {
        type: "proxy_ban",
        at: new Date().toISOString(),
        proxy: candidate.label,
        cooldown_ms: banMs,
        status: error?.status ?? null,
      };

      if (candidates.some((item) => item.key !== candidate.key)) {
        summary.remote_rotations_completed += 1;
        nextCandidateIndex = (candidates.findIndex((item) => item.key === candidate.key) + 1) % candidates.length;
        return true;
      }

      return false;
    },

    noteProxyError(candidate, error) {
      if (!candidate) {
        return;
      }
      candidate.errorHits += 1;
      candidate.cooldownUntil = Math.max(
        candidate.cooldownUntil,
        Date.now() + Math.max(5000, Math.min(options.transientErrorCooldownMs, 60000))
      );
      summary.last_proxy_error = {
        at: new Date().toISOString(),
        proxy: candidate.label,
        message: error?.message || String(error),
      };
    },

    getSummary() {
      return {
        ...summary,
        candidates_preview: candidates.slice(0, 5).map((candidate) => ({
          label: candidate.label,
          remote: candidate.remote,
          relay_code: candidate.relayCode,
          in_flight: candidate.inFlight,
          total_leases: candidate.totalLeases,
          rate_limit_hits: candidate.rateLimitHits,
          error_hits: candidate.errorHits,
          cooldown_remaining_ms: Math.max(0, candidate.cooldownUntil - Date.now()),
        })),
      };
    },
  };

  async function selectAvailableProxy() {
    while (true) {
      const now = Date.now();
      let cooldownWaitMs = Number.POSITIVE_INFINITY;
      let hasCapacityBlock = false;

      for (let offset = 0; offset < candidates.length; offset += 1) {
        const index = (nextCandidateIndex + offset) % candidates.length;
        const candidate = candidates[index];

        if (candidate.cooldownUntil > now) {
          cooldownWaitMs = Math.min(cooldownWaitMs, candidate.cooldownUntil - now);
          continue;
        }

        if (candidate.inFlight >= options.mullvadProxyMaxInFlight) {
          hasCapacityBlock = true;
          continue;
        }

        nextCandidateIndex = (index + 1) % candidates.length;
        return candidate;
      }

      const waitMs =
        cooldownWaitMs !== Number.POSITIVE_INFINITY
          ? Math.max(50, Math.min(cooldownWaitMs, 500))
          : hasCapacityBlock
            ? 50
            : 250;
      await sleep(waitMs);
    }
  }

  function getInFlightTotal() {
    return candidates.reduce((total, candidate) => total + candidate.inFlight, 0);
  }
}

function createProxyCandidate(params) {
  return {
    key: params.key,
    label: params.label,
    uri: params.uri,
    remote: params.remote,
    relayCode: params.relayCode || null,
    cooldownUntil: 0,
    rateLimitHits: 0,
    errorHits: 0,
    successes: 0,
    inFlight: 0,
    totalLeases: 0,
    agent: new params.SocksProxyAgent(params.uri),
  };
}

function buildSocksProxyUri(host, port) {
  return `socks5h://${host}:${port}`;
}

function resolveMullvadProxyCountry(options, status) {
  if (options.mullvadProxyCountry === "any") {
    return "any";
  }

  if (options.mullvadProxyCountry !== "auto") {
    return options.mullvadProxyCountry;
  }

  return status?.relay_code?.slice(0, 2) || "any";
}

async function getMullvadWireGuardRelayCodes(options, countryFilter) {
  const { stdout } = await runCommand(
    options.mullvadCommand,
    ["relay", "list"],
    options.mullvadReconnectTimeoutMs
  );

  const relayCodes = Array.from(
    new Set(
      String(stdout || "")
        .split(/\r?\n/)
        .map((line) => line.match(/^\s+([a-z]{2}-[a-z0-9]+-wg-\d{3})\s+\(/i)?.[1]?.toLowerCase())
        .filter(Boolean)
        .filter((relayCode) => countryFilter === "any" || relayCode.startsWith(`${countryFilter}-`))
    )
  );

  if (relayCodes.length === 0) {
    throw new Error(
      `No Mullvad WireGuard relays found for proxy country filter "${countryFilter}".`
    );
  }

  return relayCodes;
}

function toMullvadSocksHostname(relayCode) {
  return `${relayCode.replace(/-wg-(\d+)$/i, "-wg-socks5-$1")}.relays.mullvad.net`;
}

function shuffleArray(items) {
  const copy = items.slice();
  for (let index = copy.length - 1; index > 0; index -= 1) {
    const swapIndex = Math.floor(Math.random() * (index + 1));
    [copy[index], copy[swapIndex]] = [copy[swapIndex], copy[index]];
  }
  return copy;
}

function createRateController(options, vpnController, proxyController) {
  const state = {
    enabled: true,
    minRequestGapMs: options.minRequestGapMs,
    currentRequestGapMs: options.minRequestGapMs,
    maxRequestGapMs: options.maxRequestGapMs,
    initialRateLimitCooldownMs: options.rateLimitCooldownMs,
    currentRateLimitCooldownMs: options.rateLimitCooldownMs,
    maxRateLimitCooldownMs: options.maxRateLimitCooldownMs,
    transientErrorCooldownMs: options.transientErrorCooldownMs,
    recoverySuccessCount: DEFAULT_RECOVERY_SUCCESS_COUNT,
    nextRequestAt: 0,
    pauseUntil: 0,
    totalReservations: 0,
    totalWaitMs: 0,
    consecutiveSuccesses: 0,
    consecutiveRateLimits: 0,
    rateLimitHits: 0,
    proxyHandledRateLimitHits: 0,
    transientFailureHits: 0,
    lastEvent: null,
  };

  let reservationQueue = Promise.resolve();

  return {
    async reserve(label) {
      const reservation = reservationQueue.then(async () => {
        while (true) {
          const now = Date.now();
          const waitMs = Math.max(0, state.pauseUntil - now, state.nextRequestAt - now);

          if (waitMs <= 0) {
            if (vpnController) {
              await vpnController.waitForReady();
            }
            if (proxyController) {
              await proxyController.waitForReady();
            }

            const postVpnNow = Date.now();
            const postVpnWaitMs = Math.max(
              0,
              state.pauseUntil - postVpnNow,
              state.nextRequestAt - postVpnNow
            );
            if (postVpnWaitMs > 0) {
              state.totalWaitMs += postVpnWaitMs;
              await sleep(postVpnWaitMs);
              continue;
            }

            const grantedAt = Date.now();
            state.nextRequestAt = grantedAt + state.currentRequestGapMs;
            state.totalReservations += 1;
            state.lastEvent = {
              type: "reserve",
              at: new Date(grantedAt).toISOString(),
              label,
            };
            return;
          }

          state.totalWaitMs += waitMs;
          await sleep(waitMs);
        }
      });

      reservationQueue = reservation.catch(() => {});
      return reservation;
    },

    noteSuccess() {
      state.consecutiveSuccesses += 1;
      state.consecutiveRateLimits = 0;

      if (
        state.consecutiveSuccesses >= state.recoverySuccessCount &&
        state.currentRequestGapMs > state.minRequestGapMs
      ) {
        state.currentRequestGapMs = Math.max(
          state.minRequestGapMs,
          Math.floor(state.currentRequestGapMs * 0.9)
        );
        state.currentRateLimitCooldownMs = Math.max(
          state.initialRateLimitCooldownMs,
          Math.floor(state.currentRateLimitCooldownMs * 0.85)
        );
        state.consecutiveSuccesses = 0;
        state.lastEvent = {
          type: "recover",
          at: new Date().toISOString(),
          current_request_gap_ms: state.currentRequestGapMs,
          current_rate_limit_cooldown_ms: state.currentRateLimitCooldownMs,
        };
      }
    },

    noteRateLimit(error) {
      const gapStepMs = Math.max(50, state.minRequestGapMs);
      state.rateLimitHits += 1;
      state.consecutiveRateLimits += 1;
      state.consecutiveSuccesses = 0;

      const cooldownMs = Math.min(
        state.maxRateLimitCooldownMs,
        Math.max(error?.retryAfterMs || 0, state.currentRateLimitCooldownMs)
      );

      state.pauseUntil = Math.max(state.pauseUntil, Date.now() + cooldownMs);
      state.currentRequestGapMs = Math.min(
        state.maxRequestGapMs,
        Math.max(
          gapStepMs,
          state.currentRequestGapMs + gapStepMs,
          Math.floor(state.currentRequestGapMs * 1.5)
        )
      );
      state.currentRateLimitCooldownMs = Math.min(
        state.maxRateLimitCooldownMs,
        Math.max(
          state.initialRateLimitCooldownMs,
          Math.floor(state.currentRateLimitCooldownMs * 2)
        )
      );
      state.lastEvent = {
        type: "rate_limit",
        at: new Date().toISOString(),
        status: error?.status || null,
        cooldown_ms: cooldownMs,
        current_request_gap_ms: state.currentRequestGapMs,
        retry_after_ms: error?.retryAfterMs ?? null,
      };
    },

    async handleRateLimit(error, meta) {
      if (proxyController) {
        const rotated = await proxyController.handleRateLimit(error, meta).catch(() => false);
        if (rotated) {
          state.rateLimitHits += 1;
          state.proxyHandledRateLimitHits += 1;
          state.consecutiveSuccesses = 0;
          state.consecutiveRateLimits = 0;
          state.lastEvent = {
            type: "proxy_rate_limit",
            at: new Date().toISOString(),
            status: error?.status || null,
            retry_after_ms: error?.retryAfterMs ?? null,
            proxy_key: meta?.proxyKey ?? null,
          };
          return;
        }
      }

      this.noteRateLimit(error);

      if (vpnController) {
        try {
          await vpnController.rotate({
            reason: "rate_limit",
            status: error?.status ?? null,
            retry_after_ms: error?.retryAfterMs ?? null,
            profile_id: meta?.profileId ?? null,
            attempt: meta?.attempt ?? null,
          });
        } catch (rotationError) {
          state.lastEvent = {
            type: "vpn_rotation_failed",
            at: new Date().toISOString(),
            message: rotationError?.message || String(rotationError),
            current_request_gap_ms: state.currentRequestGapMs,
          };
        }
      }
    },

    noteTransientFailure(error) {
      const gapStepMs = Math.max(100, state.minRequestGapMs);
      state.transientFailureHits += 1;
      state.consecutiveSuccesses = 0;

      const cooldownMs = Math.max(0, state.transientErrorCooldownMs);
      if (cooldownMs > 0) {
        state.pauseUntil = Math.max(state.pauseUntil, Date.now() + cooldownMs);
      }

      state.currentRequestGapMs = Math.min(
        state.maxRequestGapMs,
        Math.max(state.minRequestGapMs, state.currentRequestGapMs + gapStepMs)
      );
      state.lastEvent = {
        type: "transient_failure",
        at: new Date().toISOString(),
        status: error?.status || null,
        cooldown_ms: cooldownMs,
        current_request_gap_ms: state.currentRequestGapMs,
        error_name: error?.name || "Error",
      };
    },

    getSummary() {
      return {
        enabled: state.enabled,
        min_request_gap_ms: state.minRequestGapMs,
        current_request_gap_ms: state.currentRequestGapMs,
        max_request_gap_ms: state.maxRequestGapMs,
        rate_limit_cooldown_ms: state.currentRateLimitCooldownMs,
        max_rate_limit_cooldown_ms: state.maxRateLimitCooldownMs,
        transient_error_cooldown_ms: state.transientErrorCooldownMs,
        pause_remaining_ms: Math.max(0, state.pauseUntil - Date.now()),
        next_request_in_ms: Math.max(0, state.nextRequestAt - Date.now()),
        total_reservations: state.totalReservations,
        total_wait_ms: state.totalWaitMs,
        rate_limit_hits: state.rateLimitHits,
        proxy_handled_rate_limit_hits: state.proxyHandledRateLimitHits,
        transient_failure_hits: state.transientFailureHits,
        consecutive_rate_limits: state.consecutiveRateLimits,
        last_event: state.lastEvent,
      };
    },
  };
}

async function fetchProfileWithRetries(params) {
  const {
    profileId,
    apiUrl,
    timeoutMs,
    retries,
    retryBaseMs,
    cookieHeader,
    rateController,
    proxyController,
  } = params;

  let attempt = 0;
  let lastError = null;

  while (attempt <= retries) {
    attempt += 1;
    const startedAt = Date.now();
    let proxyLease = null;

    try {
      await rateController.reserve(`profile:${profileId}:attempt:${attempt}`);
      if (proxyController) {
        proxyLease = await proxyController.acquire(`profile:${profileId}:attempt:${attempt}`);
      }

      const headers = {
        accept: "application/json, text/plain, */*",
        "user-agent": USER_AGENT,
      };

      if (cookieHeader) {
        headers.cookie = cookieHeader;
      }

      const response = await requestText(apiUrl, {
        headers,
        timeoutMs,
        agent: proxyLease?.agent || null,
      });

      const contentType = response.headers["content-type"] || "";
      const text = response.body;

      if (!response.ok) {
        const error = new Error(
          `HTTP ${response.status} for profile ${profileId}${text ? `: ${trimPreview(text)}` : ""}`
        );
        error.name = "HttpError";
        error.status = response.status;
        error.responseText = trimPreview(text);
        error.retryAfterMs = parseRetryAfter(response.headers["retry-after"]);
        throw error;
      }

      let data;
      try {
        data = JSON.parse(text);
      } catch (error) {
        const parseError = new Error(
          `Invalid JSON for profile ${profileId}: ${error.message}`
        );
        parseError.name = "InvalidJsonError";
        parseError.responseText = trimPreview(text);
        throw parseError;
      }

      rateController.noteSuccess();
      if (proxyController) {
        proxyController.noteSuccess(proxyLease);
      }

      return {
        data,
        attempts: attempt,
        status: response.status,
        durationMs: Date.now() - startedAt,
        fetchedAt: new Date().toISOString(),
        etag: response.headers.etag || null,
        lastModified: response.headers["last-modified"] || null,
        contentType,
        proxyLabel: proxyLease?.label || null,
        proxyUri: proxyLease?.uri || null,
      };
    } catch (error) {
      lastError = error;

      if (proxyController && proxyLease && !isRateLimitError(error)) {
        proxyController.noteProxyError(proxyLease, error);
      }

      if (isRateLimitError(error)) {
        await rateController.handleRateLimit(error, {
          profileId,
          attempt,
          proxyKey: proxyLease?.key || null,
        });
      } else if (isRetriableError(error) && (!proxyController || !proxyLease)) {
        rateController.noteTransientFailure(error);
      }

      const shouldRetry = isRetriableError(error) && attempt <= retries;

      if (!shouldRetry) {
        break;
      }

      const backoffMs = getBackoffMs({
        attempt,
        retryBaseMs,
        retryAfterMs: error.retryAfterMs,
      });

      await sleep(backoffMs);
    } finally {
      if (proxyController && proxyLease) {
        proxyController.release(proxyLease);
      }
    }
  }

  throw lastError || new Error(`Unknown error while fetching profile ${profileId}`);
}

function isRetriableError(error) {
  if (!error) {
    return false;
  }

  if (error.name === "TimeoutError" || error.name === "AbortError") {
    return true;
  }

  if (error.name === "InvalidJsonError") {
    return true;
  }

  if (typeof error.status === "number") {
    return error.status === 408 || error.status === 409 || error.status === 425 || error.status === 429 || error.status >= 500;
  }

  return true;
}

function isRateLimitError(error) {
  if (!error) {
    return false;
  }

  if (error.status === 429) {
    return true;
  }

  if (error.status === 403 || error.status === 503) {
    const text = String(error.responseText || "").toLowerCase();
    return (
      text.includes("rate limit") ||
      text.includes("too many requests") ||
      text.includes("temporarily blocked") ||
      text.includes("captcha") ||
      text.includes("access denied") ||
      text.includes("cloudflare")
    );
  }

  return false;
}

function getBackoffMs({ attempt, retryBaseMs, retryAfterMs }) {
  if (retryAfterMs != null && retryAfterMs >= 0) {
    return Math.min(retryAfterMs, 120000);
  }

  const base = retryBaseMs * 2 ** Math.max(0, attempt - 1);
  const jitter = Math.floor(Math.random() * Math.max(250, Math.round(base * 0.25)));
  return Math.min(base + jitter, 30000);
}

function parseRetryAfter(headerValue) {
  if (!headerValue) {
    return null;
  }

  const seconds = Number.parseInt(headerValue, 10);
  if (Number.isInteger(seconds) && seconds >= 0) {
    return seconds * 1000;
  }

  const absoluteTime = Date.parse(headerValue);
  if (!Number.isNaN(absoluteTime)) {
    return Math.max(0, absoluteTime - Date.now());
  }

  return null;
}

async function buildAggregateOutputs(params) {
  const { profiles, outputDir, profilesRoot, detailsNdjsonFile, flatCsvFile } = params;

  const csvColumns = [
    "profile_id",
    "source_count",
    "source_states",
    "source_user_ids",
    "profile_page_url",
    "profile_api_url",
    "public_name",
    "first_name",
    "last_name",
    "title",
    "introduction_headline",
    "introduction",
    "location",
    "city",
    "state",
    "country",
    "country_name",
    "timezone",
    "availability_for_work",
    "availability_for_work_options",
    "approved",
    "can_bid",
    "account",
    "role",
    "experience_level",
    "total_jobs",
    "review_count",
    "average_rating",
    "average_work_quality",
    "average_responsiveness",
    "profile_visits_count",
    "primary_role",
    "roles",
    "skill_count",
    "skills",
    "superpower_skills",
    "work_experience_count",
    "work_experience",
    "school_count",
    "schools",
    "certificate_count",
    "certificates",
    "portfolio_count",
    "external_profile_count",
    "external_profiles",
    "space_count",
    "spaces",
    "created_at",
    "fetched_at",
    "output_file",
  ];

  const ndjsonStream = fs.createWriteStream(detailsNdjsonFile, { encoding: "utf8" });
  const csvStream = fs.createWriteStream(flatCsvFile, { encoding: "utf8" });
  csvStream.write(`${csvColumns.join(",")}\n`);

  let present = 0;
  let missing = 0;

  for (const entry of profiles) {
    const profileFile = getProfileFilePath(profilesRoot, entry.profile_id);
    if (!(await fileExistsWithContent(profileFile))) {
      missing += 1;
      continue;
    }

    const wrapper = JSON.parse(await fsp.readFile(profileFile, "utf8"));
    wrapper.output_file = path.relative(outputDir, profileFile);

    ndjsonStream.write(`${JSON.stringify(wrapper)}\n`);

    const flatRecord = flattenProfileRecord(wrapper);
    const csvLine = csvColumns.map((column) => csvEscape(flatRecord[column] ?? "")).join(",");
    csvStream.write(`${csvLine}\n`);

    present += 1;
  }

  await Promise.all([waitForStream(ndjsonStream), waitForStream(csvStream)]);

  return {
    profiles_written_to_ndjson: present,
    profiles_missing_json: missing,
    details_ndjson: detailsNdjsonFile,
    flat_csv: flatCsvFile,
  };
}

function flattenProfileRecord(wrapper) {
  const profile = wrapper.profile || {};
  const user = profile.user || {};
  const address = user.address || {};
  const roles = toArray(profile.roles);
  const skills = toArray(profile.freelancer_skills);
  const workExperience = toArray(profile.freelancer_work_experience);
  const schools = toArray(profile.freelancer_schools);
  const certificates = toArray(profile.freelancer_certificates);
  const portfolioItems = toArray(profile.portfolio_items);
  const externalProfiles = toArray(profile.external_profiles);
  const spaces = toArray(profile.spaces);
  const sources = toArray(wrapper.sources);

  return {
    profile_id: wrapper.profile_id || profile.id || "",
    source_count: sources.length,
    source_states: joinValues(sources.map((item) => item.state)),
    source_user_ids: joinValues(sources.map((item) => item.user_id)),
    profile_page_url: wrapper.profile_page_url || user.profile_url || "",
    profile_api_url: wrapper.profile_api_url || "",
    public_name: user.public_name || "",
    first_name: user.first_name || "",
    last_name: user.last_name || "",
    title: user.title || "",
    introduction_headline: user.introduction_headline || "",
    introduction: cleanInlineText(user.introduction || ""),
    location: profile.location || "",
    city: address.city || "",
    state: address.state || "",
    country: address.country || "",
    country_name: address.country_name || "",
    timezone: user.timezone || "",
    availability_for_work: boolToText(profile.availability_for_work),
    availability_for_work_options: joinValues(profile.availability_for_work_options || []),
    approved: boolToText(profile.approved),
    can_bid: boolToText(profile.can_bid),
    account:
      typeof profile.account === "object"
        ? profile.account?.name || profile.account?.id || ""
        : profile.account || "",
    role:
      typeof profile.role === "object"
        ? profile.role?.name || profile.role?.id || ""
        : profile.role || "",
    experience_level: profile.experience_level || "",
    total_jobs: numberOrText(profile.total_jobs),
    review_count: numberOrText(profile.review_count),
    average_rating: numberOrText(profile.average_rating),
    average_work_quality: numberOrText(profile.average_work_quality),
    average_responsiveness: numberOrText(profile.average_responsiveness),
    profile_visits_count: numberOrText(profile.profile_visits_count),
    primary_role:
      roles.find((item) => item.primary)?.role?.name || roles[0]?.role?.name || "",
    roles: joinValues(
      roles.map((item) => {
        const roleName = item?.role?.name || "";
        const years = item?.years_experience != null ? `${item.years_experience}y` : "";
        const primary = item?.primary ? "primary" : "";
        return [roleName, years, primary].filter(Boolean).join(" ");
      })
    ),
    skill_count: skills.length,
    skills: joinValues(skills.map((item) => item?.skill?.name)),
    superpower_skills: joinValues(
      skills.filter((item) => item?.is_superpower).map((item) => item?.skill?.name)
    ),
    work_experience_count: workExperience.length,
    work_experience: joinValues(
      workExperience.map((item) =>
        cleanInlineText([item?.title, item?.company?.name].filter(Boolean).join(" @ "))
      )
    ),
    school_count: schools.length,
    schools: joinValues(
      schools.map((item) =>
        cleanInlineText([item?.degree?.name, item?.school?.name].filter(Boolean).join(" @ "))
      )
    ),
    certificate_count: certificates.length,
    certificates: joinValues(
      certificates.map((item) => item?.certificate?.name || item?.name || "")
    ),
    portfolio_count: portfolioItems.length,
    external_profile_count: externalProfiles.length,
    external_profiles: joinValues(
      externalProfiles.map((item) =>
        cleanInlineText([item?.site?.name, item?.public_url].filter(Boolean).join("="))
      )
    ),
    space_count: spaces.length,
    spaces: joinValues(spaces.map((item) => item?.name || item?.slug || item?.id)),
    created_at: user.created || "",
    fetched_at: wrapper.fetched_at || "",
    output_file: wrapper.output_file || "",
  };
}

function toArray(value) {
  return Array.isArray(value) ? value : [];
}

function joinValues(values) {
  return values
    .filter((value) => value != null && String(value).trim() !== "")
    .map((value) => cleanInlineText(String(value)))
    .join(" | ");
}

function cleanInlineText(value) {
  return String(value || "")
    .replace(/\r?\n+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function boolToText(value) {
  if (value == null) {
    return "";
  }
  return value ? "true" : "false";
}

function numberOrText(value) {
  return value == null ? "" : String(value);
}

function csvEscape(value) {
  const text = String(value ?? "");
  if (/[",\r\n]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

async function readJsonIfExists(filePath) {
  try {
    return JSON.parse(await fsp.readFile(filePath, "utf8"));
  } catch (error) {
    if (error && error.code === "ENOENT") {
      return null;
    }
    throw error;
  }
}

function clampNumber(value, min, max) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return min;
  }
  return Math.max(min, Math.min(max, Math.floor(parsed)));
}

async function runCommand(command, args, timeoutMs) {
  try {
    return await execFileAsync(command, args, {
      timeout: timeoutMs,
      windowsHide: true,
      maxBuffer: 10 * 1024 * 1024,
    });
  } catch (error) {
    const stdout = error?.stdout ? String(error.stdout).trim() : "";
    const stderr = error?.stderr ? String(error.stderr).trim() : "";
    const detail = [stdout, stderr].filter(Boolean).join(" | ");
    throw new Error(
      `Command failed: ${command} ${args.join(" ")}${detail ? ` -> ${detail}` : ""}`
    );
  }
}

async function getSocksProxyAgentClass() {
  if (SocksProxyAgentClass) {
    return SocksProxyAgentClass;
  }

  ({ SocksProxyAgent: SocksProxyAgentClass } = await import("socks-proxy-agent"));
  return SocksProxyAgentClass;
}

async function requestText(url, options) {
  const { headers, timeoutMs, agent } = options;
  const parsedUrl = new URL(url);
  const transport = parsedUrl.protocol === "https:" ? https : http;

  return await new Promise((resolve, reject) => {
    const request = transport.request(
      parsedUrl,
      {
        method: "GET",
        headers,
        agent: agent || undefined,
      },
      (response) => {
        response.setEncoding("utf8");
        let body = "";

        response.on("data", (chunk) => {
          body += chunk;
        });

        response.on("end", () => {
          resolve({
            ok: response.statusCode >= 200 && response.statusCode < 300,
            status: response.statusCode || 0,
            headers: response.headers || {},
            body,
          });
        });
      }
    );

    const handleTimeout = () => {
      const error = new Error(`Request timed out after ${timeoutMs} ms`);
      error.name = "TimeoutError";
      request.destroy(error);
    };

    request.setTimeout(timeoutMs, handleTimeout);
    request.on("error", reject);
    request.end();
  });
}

async function getMullvadStatus(options) {
  const { stdout } = await runCommand(
    options.mullvadCommand,
    ["status"],
    options.mullvadReconnectTimeoutMs
  );
  return parseMullvadStatus(stdout);
}

function parseMullvadStatus(text) {
  const raw = String(text || "").trim();
  const relay = raw.match(/Relay:\s+(.+)$/m)?.[1]?.trim() || null;
  const relayCode = relay?.match(/^([a-z]{2}-[a-z0-9]+-wg-\d{3})\b/i)?.[1]?.toLowerCase() || null;
  const ipv4 = raw.match(/IPv4:\s+([0-9.]+)/m)?.[1]?.trim() || null;
  const state = raw.split(/\r?\n/, 1)[0]?.trim() || null;

  return {
    raw,
    state,
    connected: /^Connected$/i.test(state || ""),
    relay,
    relay_code: relayCode,
    ipv4,
  };
}

function isMullvadStatusDifferent(before, after) {
  if (!after || !after.connected) {
    return false;
  }

  if (!before) {
    return true;
  }

  if (before.ipv4 && after.ipv4 && before.ipv4 !== after.ipv4) {
    return true;
  }

  if (before.relay && after.relay && before.relay !== after.relay) {
    return true;
  }

  return before.raw !== after.raw;
}

function trimPreview(value, maxLength = 300) {
  const text = cleanInlineText(value);
  return text.length <= maxLength ? text : `${text.slice(0, maxLength)}...`;
}

function redactMongoUri(uri) {
  return String(uri || "").replace(/\/\/([^:/?#]+):([^@/]+)@/, "//$1:***@");
}

function serializeError(error) {
  return {
    name: error?.name || "Error",
    message: error?.message || String(error),
    status: error?.status,
    retry_after_ms: error?.retryAfterMs,
    response_text: error?.responseText,
    stack: error?.stack,
  };
}

function sanitizeOptionsForManifest(options) {
  return {
    input: options.input || null,
    out: options.out || null,
    concurrency: options.concurrency,
    timeout_ms: options.timeoutMs,
    retries: options.retries,
    retry_base_ms: options.retryBaseMs,
    log_every: options.logEvery,
    max_errors: options.maxErrors,
    min_request_gap_ms: options.minRequestGapMs,
    max_request_gap_ms: options.maxRequestGapMs,
    rate_limit_cooldown_ms: options.rateLimitCooldownMs,
    max_rate_limit_cooldown_ms: options.maxRateLimitCooldownMs,
    transient_error_cooldown_ms: options.transientErrorCooldownMs,
    resume: options.resume,
    resume_lookback: options.resumeLookback,
    mullvad_rotate_on_rate_limit: options.mullvadRotateOnRateLimit,
    mullvad_command: options.mullvadCommand,
    mullvad_reconnect_attempts: options.mullvadReconnectAttempts,
    mullvad_reconnect_timeout_ms: options.mullvadReconnectTimeoutMs,
    mullvad_settle_ms: options.mullvadSettleMs,
    mullvad_proxy_mode: options.mullvadProxyMode,
    mullvad_proxy_host: options.mullvadProxyHost,
    mullvad_proxy_port: options.mullvadProxyPort,
    mullvad_proxy_country: options.mullvadProxyCountry,
    mullvad_proxy_max_relays: options.mullvadProxyMaxRelays,
    mullvad_proxy_max_inflight: options.mullvadProxyMaxInFlight,
    high_speed_mode: options.highSpeedMode,
    mongo_enabled: options.mongoEnabled,
    mongo_uri: redactMongoUri(options.mongoUri),
    mongo_db: options.mongoDb,
    mongo_collection: options.mongoCollection,
    mongo_timeout_ms: options.mongoTimeoutMs,
    offset: options.offset,
    limit: options.limit,
    force: options.force,
    summary_only: options.summaryOnly,
    cookie_supplied: Boolean(options.cookie || options.cookieFile),
  };
}

function getProfileFilePath(profilesRoot, profileId) {
  const bucket = String(profileId).padStart(6, "0").slice(0, 2);
  return path.join(profilesRoot, bucket, `${profileId}.json`);
}

async function writeJsonAtomic(filePath, value) {
  await ensureDir(path.dirname(filePath));
  const tempPath = `${filePath}.${process.pid}.${Date.now()}.tmp`;
  await fsp.writeFile(tempPath, `${JSON.stringify(value, null, 2)}\n`, "utf8");
  await fsp.rename(tempPath, filePath);
}

async function appendNdjson(filePath, value) {
  await ensureDir(path.dirname(filePath));
  await fsp.appendFile(filePath, `${JSON.stringify(value)}\n`, "utf8");
}

async function ensureDir(dirPath) {
  await fsp.mkdir(dirPath, { recursive: true });
}

async function fileExistsWithContent(filePath) {
  try {
    const stats = await fsp.stat(filePath);
    return stats.isFile() && stats.size > 2;
  } catch (error) {
    if (error && error.code === "ENOENT") {
      return false;
    }
    throw error;
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForStream(stream) {
  await new Promise((resolve, reject) => {
    stream.on("error", reject);
    stream.end(() => resolve());
  });
}

main().catch((error) => {
  console.error(error?.stack || String(error));
  process.exit(1);
});
