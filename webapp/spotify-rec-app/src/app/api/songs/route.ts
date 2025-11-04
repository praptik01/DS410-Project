import { NextRequest, NextResponse } from "next/server";
import path from "path";
import { access } from "fs/promises";
import { createReadStream } from "fs";
import readline from "readline";

async function resolveDatasetPath(filename: string) {
  let currentDir = process.cwd();

  // Walk up the directory tree to locate the dataset file.
  // Next.js sometimes runs from a workspace root rather than the app directory,
  // so we defensively search parent directories.
  for (;;) {
    const candidate = path.join(currentDir, filename);
    try {
      await access(candidate);
      return candidate;
    } catch {
      const parent = path.dirname(currentDir);
      if (parent === currentDir) {
        break;
      }
      currentDir = parent;
    }
  }

  throw new Error(`Unable to locate ${filename} from ${process.cwd()}`);
}

const DEFAULT_LIMIT = 40;
const MAX_LIMIT = 200;

function parseCsvLine(line: string): string[] {
  const values: string[] = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const char = line[i];
    if (char === '"') {
      const peek = line[i + 1];
      if (inQuotes && peek === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (char === "," && !inQuotes) {
      values.push(current);
      current = "";
    } else {
      current += char;
    }
  }

  values.push(current);
  return values;
}

type SongResult = {
  title: string;
  artist: string;
};

async function loadSongMatches(
  csvPath: string,
  query: string,
  limit: number,
): Promise<SongResult[]> {
  const normalizedQuery = query.trim().toLowerCase();
  const results: SongResult[] = [];
  const seen = new Set<string>();
  const stream = createReadStream(csvPath, { encoding: "utf8" });
  const rl = readline.createInterface({
    input: stream,
    crlfDelay: Infinity,
  });

  let songIndex = -1;
  let artistIndex = -1;
  let processedHeader = false;

  try {
    for await (const rawLine of rl) {
      const line = rawLine.trimEnd();
      if (!line) {
        continue;
      }

      if (!processedHeader) {
        const header = parseCsvLine(line.replace(/^\ufeff/, ""));
        songIndex = header.findIndex(
          (column) => column.trim().toLowerCase() === "song",
        );
        artistIndex = header.findIndex(
          (column) => column.trim().toLowerCase() === "artist(s)",
        );
        if (songIndex === -1) {
          throw new Error("Song column not found in spotify_dataset.csv");
        }
        processedHeader = true;
        continue;
      }

      const values = parseCsvLine(line);
      if (values.length <= songIndex) {
        continue;
      }
      const title = values[songIndex]?.trim();
      if (!title) {
        continue;
      }
      const artist = artistIndex >= 0 ? values[artistIndex]?.trim() ?? "" : "";
      const identity = `${title.toLowerCase()}__${artist.toLowerCase()}`;
      if (seen.has(identity)) {
        continue;
      }

      if (
        normalizedQuery &&
        !`${title} ${artist}`.toLowerCase().includes(normalizedQuery)
      ) {
        continue;
      }

      seen.add(identity);
      results.push({ title, artist });

      if (results.length >= limit) {
        break;
      }
    }
  } finally {
    rl.close();
    stream.close();
  }

  return results;
}

export async function GET(request: NextRequest) {
  try {
    const url = new URL(request.url);
    const query = url.searchParams.get("q") ?? "";
    const limitParam = Number.parseInt(url.searchParams.get("limit") ?? "", 10);
    const sanitizedLimit =
      Number.isFinite(limitParam) && limitParam > 0
        ? Math.min(limitParam, MAX_LIMIT)
        : DEFAULT_LIMIT;

    const csvPath = await resolveDatasetPath("spotify_dataset.csv");
    const songs = await loadSongMatches(csvPath, query, sanitizedLimit);
    return NextResponse.json({ songs });
  } catch (error) {
    console.error("Failed to load songs:", error);
    return NextResponse.json(
      { error: "Unable to load songs from dataset." },
      { status: 500 },
    );
  }
}
