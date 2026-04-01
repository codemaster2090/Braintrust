#!/usr/bin/env node
"use strict";

const fs = require("fs");
const fsp = fs.promises;
const path = require("path");
let MongoClient;

try {
  ({ MongoClient } = require("mongodb"));
} catch (error) {
  MongoClient = null;
}

const BASE_URL = "https://app.usebraintrust.com";
const API_PREFIX = "/api/freelancers/";
const DEFAULT_CONCURRENCY = 4;
const DEFAULT_TIMEOUT_MS = 30000;
const DEFAULT_RETRIES = 5;
const DEFAULT_RETRY_BASE_MS = 1000;
const DEFAULT_LOG_EVERY = 25;
const DEFAULT_MAX_ERRORS = 1000;
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
  let mongoSink = null;

  try {
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

    await fsp.writeFile(errorsFile, "", "utf8");

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

    const aggregateSummary = await buildAggregateOutputs({
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
      invalid_rows_preview: parsed.invalidRows.slice(0, 25),
    };

    await writeJsonAtomic(progressFile, {
      started_at: runStartedAt,
      completed_at: finalManifest.completed_at,
      status: downloadSummary.failed > 0 ? "completed_with_errors" : "completed",
      ...downloadSummary,
      mongo: mongoSink ? mongoSink.getSummary() : null,
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
    console.log(`NDJSON:        ${detailsNdjsonFile}`);
    console.log(`Flat CSV:      ${flatCsvFile}`);

    if (downloadSummary.failed > 0) {
      process.exitCode = 2;
    }
  } finally {
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
        break;
      case "timeout-ms":
        options.timeoutMs = toPositiveInteger(nextValue, "--timeout-ms");
        break;
      case "retries":
        options.retries = toNonNegativeInteger(nextValue, "--retries");
        break;
      case "retry-base-ms":
        options.retryBaseMs = toPositiveInteger(nextValue, "--retry-base-ms");
        break;
      case "log-every":
        options.logEvery = toPositiveInteger(nextValue, "--log-every");
        break;
      case "max-errors":
        options.maxErrors = toPositiveInteger(nextValue, "--max-errors");
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
  node download_braintrust_profiles.js --mongo-uri mongodb://localhost:27017/ --mongo-db braintrust --mongo-collection profiles
  node download_braintrust_profiles.js --out ./run_01 --force
  node download_braintrust_profiles.js --summary-only --out ./braintrust_profile_details
  `);
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
  } = params;

  let nextIndex = 0;
  let stopRequested = false;
  let lastLoggedAt = 0;

  const summary = {
    total: profiles.length,
    downloaded: 0,
    skipped_existing: 0,
    failed: 0,
    processed: 0,
    stopped_early: false,
  };

  await writeJsonAtomic(progressFile, {
    started_at: runStartedAt,
    status: "running",
    input_csv: inputCsv,
    output_dir: outputDir,
    ...summary,
    mongo: mongoSink ? mongoSink.getSummary() : null,
  });

  const workers = Array.from({ length: options.concurrency }, (_, workerIndex) =>
    runWorker(workerIndex + 1)
  );
  await Promise.all(workers);

  async function runWorker(workerId) {
    while (!stopRequested) {
      const currentIndex = nextIndex;
      nextIndex += 1;

      if (currentIndex >= profiles.length) {
        return;
      }

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
    };

    await writeJsonAtomic(progressFile, payload);
    console.log(
      `[progress] ${summary.processed}/${summary.total} processed | downloaded=${summary.downloaded} skipped=${summary.skipped_existing} failed=${summary.failed}`
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

async function fetchProfileWithRetries(params) {
  const { profileId, apiUrl, timeoutMs, retries, retryBaseMs, cookieHeader } = params;

  let attempt = 0;
  let lastError = null;

  while (attempt <= retries) {
    attempt += 1;
    const startedAt = Date.now();

    try {
      const headers = {
        accept: "application/json, text/plain, */*",
        "user-agent": USER_AGENT,
      };

      if (cookieHeader) {
        headers.cookie = cookieHeader;
      }

      const response = await fetch(apiUrl, {
        headers,
        signal: AbortSignal.timeout(timeoutMs),
      });

      const contentType = response.headers.get("content-type") || "";
      const text = await response.text();

      if (!response.ok) {
        const error = new Error(
          `HTTP ${response.status} for profile ${profileId}${text ? `: ${trimPreview(text)}` : ""}`
        );
        error.name = "HttpError";
        error.status = response.status;
        error.responseText = trimPreview(text);
        error.retryAfterMs = parseRetryAfter(response.headers.get("retry-after"));
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

      return {
        data,
        attempts: attempt,
        status: response.status,
        durationMs: Date.now() - startedAt,
        fetchedAt: new Date().toISOString(),
        etag: response.headers.get("etag"),
        lastModified: response.headers.get("last-modified"),
        contentType,
      };
    } catch (error) {
      lastError = error;
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
