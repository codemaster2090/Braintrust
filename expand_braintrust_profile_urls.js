#!/usr/bin/env node
"use strict";

const fs = require("fs");
const fsp = fs.promises;
const path = require("path");

const BASE_URL = "https://app.usebraintrust.com";
const RELATIVE_PROFILE_PATH = /^\/talent\/\d+\/?$/i;

async function main() {
  const options = parseArgs(process.argv.slice(2));

  if (options.help) {
    printHelp();
    return;
  }

  const inputPath = options.input;
  if (!inputPath) {
    throw new Error("Missing --input <file>.");
  }

  const resolvedInputPath = path.resolve(inputPath);
  const fileExtension = path.extname(resolvedInputPath).toLowerCase();
  const outputPath = path.resolve(
    options.inPlace ? resolvedInputPath : options.output || defaultOutputPath(resolvedInputPath)
  );

  if (fileExtension === ".csv") {
    await rewriteCsvFile(resolvedInputPath, outputPath);
  } else if (fileExtension === ".json") {
    await rewriteJsonFile(resolvedInputPath, outputPath);
  } else {
    throw new Error("Unsupported input file type. Use .csv or .json.");
  }
}

function parseArgs(argv) {
  const options = {
    input: "",
    output: "",
    inPlace: false,
    help: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];

    if (arg === "--help" || arg === "-h") {
      options.help = true;
      continue;
    }

    if (arg === "--in-place") {
      options.inPlace = true;
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
      case "output":
        options.output = nextValue;
        break;
      default:
        throw new Error(`Unknown option: --${key}`);
    }
  }

  if (options.inPlace && options.output) {
    throw new Error("Use either --in-place or --output, not both.");
  }

  return options;
}

function printHelp() {
  console.log(`
Usage:
  node expand_braintrust_profile_urls.js --input <file.csv|file.json> [--output <file>]
  node expand_braintrust_profile_urls.js --input <file.csv|file.json> --in-place

What it does:
  Rewrites Braintrust profile links like /talent/2016869/ to:
  https://app.usebraintrust.com/talent/2016869/

Supported fields:
  CSV column: Profile URL
  JSON keys:  profile_url, Profile URL, profile_page_url

Examples:
  node expand_braintrust_profile_urls.js --input .\\braintrust_developers_2026-04-01T17-58-21-624Z_filtered.csv
  node expand_braintrust_profile_urls.js --input .\\braintrust_developers_2026-04-01T17-58-21-624Z_filtered.json
  node expand_braintrust_profile_urls.js --input .\\braintrust_developers_2026-04-01T17-58-21-624Z_filtered.csv --in-place
  `);
}

function defaultOutputPath(inputPath) {
  const parsed = path.parse(inputPath);
  return path.join(parsed.dir, `${parsed.name}_full_urls${parsed.ext}`);
}

async function rewriteCsvFile(inputPath, outputPath) {
  const text = await fsp.readFile(inputPath, "utf8");
  const rows = parseCsv(text);

  if (rows.length === 0) {
    throw new Error("CSV file is empty.");
  }

  const header = rows[0];
  const profileUrlIndex = header.findIndex((name) => name === "Profile URL");
  if (profileUrlIndex === -1) {
    throw new Error('CSV does not contain a "Profile URL" column.');
  }

  let changed = 0;
  for (let rowIndex = 1; rowIndex < rows.length; rowIndex += 1) {
    const row = rows[rowIndex];
    const originalValue = row[profileUrlIndex] ?? "";
    const expandedValue = toAbsoluteProfileUrl(originalValue);
    if (expandedValue !== originalValue) {
      row[profileUrlIndex] = expandedValue;
      changed += 1;
    }
  }

  await fsp.writeFile(outputPath, stringifyCsv(rows), "utf8");

  console.log(`Input:   ${inputPath}`);
  console.log(`Output:  ${outputPath}`);
  console.log(`Rows:    ${Math.max(0, rows.length - 1)}`);
  console.log(`Changed: ${changed}`);
}

async function rewriteJsonFile(inputPath, outputPath) {
  const text = await fsp.readFile(inputPath, "utf8");
  const data = JSON.parse(text);
  const stats = { changed: 0 };
  const transformed = rewriteJsonProfileUrls(data, stats);

  await fsp.writeFile(outputPath, `${JSON.stringify(transformed, null, 2)}\n`, "utf8");

  console.log(`Input:   ${inputPath}`);
  console.log(`Output:  ${outputPath}`);
  console.log(`Changed: ${stats.changed}`);
}

function rewriteJsonProfileUrls(value, stats) {
  if (Array.isArray(value)) {
    return value.map((item) => rewriteJsonProfileUrls(item, stats));
  }

  if (value && typeof value === "object") {
    const result = {};
    for (const [key, currentValue] of Object.entries(value)) {
      if (
        typeof currentValue === "string" &&
        (key === "profile_url" || key === "Profile URL" || key === "profile_page_url")
      ) {
        const expandedValue = toAbsoluteProfileUrl(currentValue);
        if (expandedValue !== currentValue) {
          stats.changed += 1;
        }
        result[key] = expandedValue;
      } else {
        result[key] = rewriteJsonProfileUrls(currentValue, stats);
      }
    }
    return result;
  }

  return value;
}

function toAbsoluteProfileUrl(value) {
  const text = String(value || "").trim();
  if (!text) {
    return "";
  }

  if (/^https?:\/\//i.test(text)) {
    return text;
  }

  if (!RELATIVE_PROFILE_PATH.test(text)) {
    return text;
  }

  return new URL(text, BASE_URL).href;
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

function stringifyCsv(rows) {
  return `${rows
    .map((row) => row.map((cell) => csvEscape(cell)).join(","))
    .join("\n")}\n`;
}

function csvEscape(value) {
  const text = String(value ?? "");
  if (/[",\r\n]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

main().catch((error) => {
  console.error(error?.stack || String(error));
  process.exit(1);
});
