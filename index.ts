import { Storage } from "@google-cloud/storage";
import fs from "fs";
import path from "path";

// Initialize Google Cloud Storage
const storage = new Storage({
  keyFilename: "key.json",
});

const BUCKET_NAME = Bun.env.BUCKET_NAME;
if (!BUCKET_NAME) throw new Error("BUCKET_NAME is not defined");
const OUTPUT_FOLDER = path.resolve(__dirname, "./data");

// Ensure output directory exists
if (!fs.existsSync(OUTPUT_FOLDER)) {
  fs.mkdirSync(OUTPUT_FOLDER, { recursive: true });
}

interface FileWithDate {
  file: any;
  created: Date;
  name: string;
}

// Get the latest processed file information
function getLatestProcessedFile(): { date: Date; sequence: number } | null {
  const files = fs.readdirSync(OUTPUT_FOLDER);
  let latestDate: Date | null = null;
  let latestSequence = -1;

  files.forEach((file) => {
    if (!file.endsWith(".json")) return;

    // Parse date from filename (DD-MM-YYYY-Sn.json)
    const match = file.match(/(\d{2})-(\d{2})-(\d{4})-S(\d+)\.json/);
    if (!match) return;

    const [_, day, month, year, sequence] = match;
    const date = new Date(parseInt(year), parseInt(month) - 1, parseInt(day));
    const seq = parseInt(sequence);

    if (!latestDate || date > latestDate) {
      latestDate = date;
      latestSequence = seq;
    } else if (
      date.getTime() === latestDate.getTime() &&
      seq > latestSequence
    ) {
      latestSequence = seq;
    }
  });

  return latestDate ? { date: latestDate, sequence: latestSequence } : null;
}

// Format date as DD-MM-YYYY
function formatDate(date: Date): string {
  return `${date.getDate().toString().padStart(2, "0")}-${(date.getMonth() + 1).toString().padStart(2, "0")}-${date.getFullYear()}`;
}

async function downloadAndSaveJSON() {
  try {
    // Get latest processed file info
    const lastProcessed = getLatestProcessedFile();
    console.log(
      "Last processed file:",
      lastProcessed
        ? `Date: ${formatDate(lastProcessed.date)}, Sequence: S${lastProcessed.sequence}`
        : "No processed files found",
    );

    // Get all JSON files from bucket
    const [files] = await storage.bucket(BUCKET_NAME).getFiles();

    // Filter and map JSON files with their creation date
    const jsonFiles: FileWithDate[] = [];

    for (const file of files) {
      if (!file.name.endsWith(".json")) continue;

      const [metadata] = await file.getMetadata();
      const created = new Date(metadata.timeCreated);

      // Only include files newer than the last processed file
      if (lastProcessed && created <= lastProcessed.date) continue;

      jsonFiles.push({
        file,
        created,
        name: file.name,
      });
    }

    // Sort files by creation date
    jsonFiles.sort((a, b) => a.created.getTime() - b.created.getTime());

    // Group files by date
    const filesByDate = new Map<string, FileWithDate[]>();

    jsonFiles.forEach((fileData) => {
      const dateStr = formatDate(fileData.created);
      if (!filesByDate.has(dateStr)) {
        filesByDate.set(dateStr, []);
      }
      filesByDate.get(dateStr)?.push(fileData);
    });

    // Process each date's files
    for (const [dateStr, dateFiles] of filesByDate) {
      console.log(`Processing files for date: ${dateStr}`);

      for (let i = 0; i < dateFiles.length; i++) {
        const { file, name } = dateFiles[i];

        try {
          // Download file contents
          const [contents] = await file.download();
          const rawText = contents.toString().trim();

          if (!rawText) {
            console.warn(`Empty file skipped: ${name}`);
            continue;
          }

          // Save JSON file with incremental sequence number
          const filename = `${dateStr}-S${i}.json`;
          const outputPath = path.join(OUTPUT_FOLDER, filename);
          fs.writeFileSync(outputPath, rawText);

          console.log(`Processed ${name} -> ${filename}`);
        } catch (err) {
          console.error(`Error processing file ${name}:`, err);
        }
      }
    }

    console.log("Processing completed");
  } catch (error) {
    console.error("Error processing files:", error);
    throw error;
  }
}

// Execute the process
downloadAndSaveJSON().catch(console.error);
