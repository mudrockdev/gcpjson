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
const TODAY_FILE = path.join(OUTPUT_FOLDER, "today.json");

// Ensure output directory exists
if (!fs.existsSync(OUTPUT_FOLDER)) {
  fs.mkdirSync(OUTPUT_FOLDER, { recursive: true });
}

interface FileWithDate {
  file: any;
  created: Date;
  name: string;
  size: number;
}

// Format date as YYYY/MM/DD for path matching
function formatDatePath(date: Date): string {
  return `${date.getUTCFullYear()}/${(date.getUTCMonth() + 1)
    .toString()
    .padStart(2, "0")}/${date.getUTCDate().toString().padStart(2, "0")}`;
}

// Check if a path contains today's date
function isFromToday(filePath: string): boolean {
  const today = new Date();
  const todayPath = formatDatePath(today);
  return filePath.includes(todayPath);
}

// Parse JSON content that might be either JSON Lines or regular JSON
function parseJsonContent(content: string): any[] {
  const trimmedContent = content.trim();
  if (!trimmedContent) return [];

  // Try parsing as a single JSON first
  try {
    const parsed = JSON.parse(trimmedContent);
    return Array.isArray(parsed) ? parsed : [parsed];
  } catch (e) {
    // If that fails, try parsing as JSON Lines
    const results = [];
    const lines = trimmedContent.split("\n");

    for (const line of lines) {
      const trimmedLine = line.trim();
      if (!trimmedLine) continue;

      try {
        results.push(JSON.parse(trimmedLine));
      } catch (err) {
        console.warn(
          `Failed to parse JSON line: ${trimmedLine.substring(0, 100)}...`,
        );
      }
    }

    return results;
  }
}

async function processLogs() {
  try {
    const today = new Date();
    const todayPath = formatDatePath(today);
    console.log(`Processing files for today's date: ${todayPath}`);

    // Get all files from bucket recursively
    const [files] = await storage.bucket(BUCKET_NAME).getFiles({
      autoPaginate: true,
    });

    // First, display directory structure
    console.log(`\nBucket structure for: ${BUCKET_NAME}`);
    const filesByDirectory = new Map<string, string[]>();

    for (const file of files) {
      const dirPath = path.dirname(file.name);
      if (!filesByDirectory.has(dirPath)) {
        filesByDirectory.set(dirPath, []);
      }
      filesByDirectory.get(dirPath)?.push(path.basename(file.name));
    }

    // Print directory structure
    for (const [dir, dirFiles] of filesByDirectory) {
      console.log(`\n${dir}/`);
      dirFiles.sort().forEach((file) => console.log(`  └─ ${file}`));
    }

    console.log(`\nTotal files in bucket: ${files.length}`);

    // Now process only today's JSON files
    console.log("\nStarting JSON processing...");

    // Filter for today's JSON files only
    const jsonFiles: FileWithDate[] = [];

    for (const file of files) {
      // Only process JSON files from today
      if (!file.name.endsWith(".json") || !isFromToday(file.name)) continue;

      const [metadata] = await file.getMetadata();
      const created = new Date(metadata.timeCreated);

      jsonFiles.push({
        file,
        created,
        name: file.name,
        size: parseInt(metadata.size),
      });
    }

    if (jsonFiles.length === 0) {
      console.log(`No JSON files found for today (${todayPath})`);
      return;
    }

    // Sort files by creation date
    jsonFiles.sort((a, b) => a.created.getTime() - b.created.getTime());

    console.log(`Processing ${jsonFiles.length} JSON files from today...`);

    // Process and combine all files
    const writeStream = fs.createWriteStream(TODAY_FILE, { flags: "w" });
    for (const { file, name } of jsonFiles) {
      try {
        // Download file contents
        const [contents] = await file.download();
        const rawText = contents.toString();

        if (!rawText.trim()) {
          console.warn(`Empty file skipped: ${name}`);
          continue;
        }

        // Parse JSON content
        const jsonData = parseJsonContent(rawText);
        if (jsonData.length > 0) {
          for (const jsonObject of jsonData) {
            writeStream.write(`${JSON.stringify(jsonObject)}\n`);
          }
          console.log(`✓ Processed ${name} (${jsonData.length} entries)`);
        } else {
          console.warn(`! No valid JSON entries found in ${name}`);
        }
      } catch (err) {
        console.error(`✗ Error processing file ${name}:`, err);
      }
    }

    writeStream.end();

    console.log(`\nProcessing completed:`);
    console.log(`- ${jsonFiles.length} files processed`);
    console.log(`- Output saved to: ${TODAY_FILE}`);
  } catch (error) {
    console.error("Error processing files:", error);
    throw error;
  }
}

// Execute the process
processLogs().catch(console.error);
