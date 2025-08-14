require("dotenv").config();
const axios = require("axios");
const fs = require("fs/promises");
const path = require("path");
const { Client } = require("@elastic/elasticsearch");
const { encode } = require("gpt-tokenizer");

const EMBEDDING_DIM = 3072;
const MAX_TOKEN_LENGTH = 8192; // Safe token limit for text-embedding-3-large

class XanoToES {
  constructor() {
    this.config = {
      xanoApiKey: process.env.XANO_API_KEY,
      workspaceId: process.env.XANO_WORKSPACE_ID,
      metaBaseURL: process.env.XANO_META_BASE_URL,
      esUrl: process.env.ELASTICSEARCH_URL,
      esIndex: process.env.ELASTICSEARCH_INDEX ,
      recordsPerPage: 2000,
      tableId: 106,
    };

    this.checkpointFile = path.join(__dirname, "checkpoint.json");
    this.errorFile = path.join(__dirname, "errors.json");
    this.lastProcessedPage = 0;
    this.lastProcessedRecordId = 0;
    this.failedRecords = [];

    // Only keep Elasticsearch client - no axios instances
    this.es = new Client({ node: this.config.esUrl });
    console.log("🚀 XanoToES initialized with config:", {
      esUrl: this.config.esUrl,
      esIndex: this.config.esIndex,
      recordsPerPage: this.config.recordsPerPage,
      tableId: this.config.tableId
    });
  }

  async loadCheckpoint() {
    console.log("📂 Loading checkpoint from", this.checkpointFile);
    try {
      const data = await fs.readFile(this.checkpointFile, "utf8");
      const parsed = JSON.parse(data);
      this.lastProcessedPage = parsed.lastProcessedPage || 0;
      this.lastProcessedRecordId = parsed.lastProcessedRecordId || 0;
      console.log("✅ Checkpoint loaded:", {
        lastProcessedPage: this.lastProcessedPage,
        lastProcessedRecordId: this.lastProcessedRecordId
      });
    } catch (error) {
      console.warn("⚠️ No checkpoint found or error loading, starting from scratch:", error.message);
      this.lastProcessedPage = 0;
      this.lastProcessedRecordId = 0;
    }
  }

  async saveCheckpoint() {
    console.log("💾 Saving checkpoint to", this.checkpointFile);
    try {
      await fs.writeFile(
        this.checkpointFile,
        JSON.stringify({
          lastProcessedPage: this.lastProcessedPage,
          lastProcessedRecordId: this.lastProcessedRecordId,
          timestamp: new Date().toISOString(),
        }, null, 2)
      );
      console.log("✅ Checkpoint saved successfully");
    } catch (error) {
      console.error("❌ Error saving checkpoint:", error.message);
    }
  }

  async logError(recordId, error) {
    console.error(`❌ Error for record ${recordId}:`, error);
    this.failedRecords.push({ id: recordId, error, time: new Date().toISOString() });
    try {
      await fs.writeFile(this.errorFile, JSON.stringify(this.failedRecords, null, 2));
      console.log("📝 Error logged to", this.errorFile);
    } catch (fileError) {
      console.error("❌ Failed to write error to file:", fileError.message);
    }
  }

  async fetchRecords(page) {
    const fields = [
      "id", "job_title", "company", "location", "location_bundesland", "location_zip",
      "job_date", "url", "website", "branche", "description", "berufsgruppe", "properties", "combined_embeddings"
    ];
    const fieldParams = `fields=${fields.join(",")}`;
    const url = `${this.config.metaBaseURL}/workspace/${this.config.workspaceId}/table/${this.config.tableId}/content?page=${page}&per_page=${this.config.recordsPerPage}&${fieldParams}`;
    
    console.log(`🌐 Fetching records from Xano, page ${page}, URL:`, url);
    try {
      // Create fresh axios instance for Xano API
      const res = await axios.get(url, {
        headers: {
          Authorization: `Bearer ${this.config.xanoApiKey}`,
          "Content-Type": "application/json",
          accept: "application/json",
        },
      });
      console.log(`✅ Fetched ${res.data.items?.length || 0} records from page ${page}`);
      return res.data.items || [];
    } catch (error) {
      console.error("❌ Failed to fetch records:", error.message);
      return [];
    }
  }

async updateEmbeddingInXano(record) {
  console.log(`🧠 Generating embedding for record ${record.id}`);

  if (record.combined_embeddings && record.combined_embeddings.length > 0) {
    console.log(`ℹ️ Record ${record.id} already has embeddings, skipping`);
    return;
  }

  const jobTitle = (record.job_title || "").trim();
  const description = (record.description || "").trim();

  if (!jobTitle) {
    console.error(`❌ No job_title for record ${record.id}, skipping`);
    await this.logError(record.id, "Missing job_title");
    return;
  }

  // Combine and tokenize
  let inputText = `${jobTitle}: ${description}`;
  let tokens = encode(inputText);

  if (tokens.length > MAX_TOKEN_LENGTH) {
    console.warn(`⚠️ Truncating input for record ${record.id} from ${tokens.length} tokens to ${MAX_TOKEN_LENGTH}`);
    tokens = tokens.slice(0, MAX_TOKEN_LENGTH);
    inputText = tokens.map(token => token.text).join('');
  }

  try {
    const openaiRes = await axios.post(
      "https://api.openai.com/v1/embeddings",
      {
        model: "text-embedding-3-large",
        input: inputText,
        dimensions: EMBEDDING_DIM,
      },
      {
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${process.env.OPENAI_API_KEY}`,
        },
      }
    );

    const vector = openaiRes.data?.data?.[0]?.embedding;

    if (!Array.isArray(vector) || vector.length !== EMBEDDING_DIM) {
      throw new Error(`Invalid embedding vector for record ${record.id}`);
    }

    console.log(`✅ Received valid embedding of length ${vector.length}`);

    await new Promise(resolve => setTimeout(resolve, 100));

    const url = `${this.config.metaBaseURL}/workspace/${this.config.workspaceId}/table/${this.config.tableId}/content/bulk/patch`;
    const payload = {
      items: [
        {
          row_id: record.id,
          updates: {
            combined_embeddings: JSON.stringify(vector),
          }
        }
      ]
    };

    await axios.post(url, payload, {
      headers: {
        Authorization: `Bearer ${this.config.xanoApiKey}`,
        "Content-Type": "application/json",
        accept: "application/json",
      }
    });

    record.combined_embeddings = vector;
    console.log(`✅ Updated embedding for record ${record.id}`);

  } catch (error) {
    console.error(`❌ Embedding update failed for record ${record.id}:`, error.message);
    if (error.response) {
      console.error("Status:", error.response.status);
      console.error("Data:", JSON.stringify(error.response.data, null, 2));
    }
    await this.logError(record.id, error.message);
  }
}



  async checkExistingRecords(recordIds) {
    console.log(`🔎 Checking for existing records in Elasticsearch: ${recordIds.length} IDs`);
    try {
      const response = await this.es.mget({
        index: this.config.esIndex,
        body: {
          ids: recordIds
        }
      });
      const existingIds = response.body.docs
        .filter(doc => doc.found)
        .map(doc => doc._id);
      console.log(`✅ Found ${existingIds.length} existing records in Elasticsearch`);
      return new Set(existingIds);
    } catch (error) {
      console.error("❌ Error checking existing records:", error.message);
      return new Set();
    }
  }

  mapToESFormat(record) {
    console.log(`🔄 Mapping record ${record.id} to Elasticsearch format`);
    return {
      id: parseInt(record.id),
      job_title: record.job_title || "",
      company: record.company || "",
      location: record.location || "",
      location_bundesland: record.location_bundesland || "",
      location_zip: record.location_zip || "",
      job_date: record.job_date || "",
      url: record.url || "",
      website: record.website || "",
      branche: record.branche || "",
      description: record.description || "",
      berufsgruppe: record.berufsgruppe || { values: [] },
      properties: record.properties || { values: [] },
      combined_embeddings: record.combined_embeddings || [],
    };
  }

  async bulkInsertToES(records) {
    console.log(`📤 Preparing to bulk insert ${records.length} records to Elasticsearch`);
    const body = records.flatMap(doc => [
      { index: { _index: this.config.esIndex, _id: doc.id } },
      doc,
    ]);

    try {
      console.log(`🚀 Sending bulk insert to Elasticsearch, index: ${this.config.esIndex}`);
      const response = await this.es.bulk({ body });
      
      if (response.body?.errors) {
        console.warn("⚠️ Some records failed to index in Elasticsearch");
        response.body.items.forEach((item) => {
          if (item.index?.error) {
            console.error(`❌ Elasticsearch error for record ${item.index._id}:`, item.index.error.reason);
            this.logError(item.index._id, `Elasticsearch Error: ${item.index.error.reason}`);
          }
        });
      } else {
        console.log(`✅ Successfully inserted ${records.length} records to Elasticsearch`);
      }
    } catch (error) {
      console.error("❌ Bulk insert to Elasticsearch failed:", error.message);
    }
  }

  async run() {
    console.log("🏁 Starting Xano to Elasticsearch sync process");
    await this.loadCheckpoint();
    const totalPages = Math.ceil(644000 / this.config.recordsPerPage);
    console.log(`📊 Total pages to process: ${totalPages}`);

    for (let page = this.lastProcessedPage + 1; page <= totalPages; page++) {
      console.log(`\n📄 Processing page ${page}/${totalPages}`);
      const records = await this.fetchRecords(page);
      
      if (records.length === 0) {
        console.warn(`⚠️ No records found on page ${page}, skipping...`);
        continue;
      }

      // Check for existing records
      const recordIds = records.map(record => record.id.toString());
      const existingIds = await this.checkExistingRecords(recordIds);
      const newRecords = records.filter(record => !existingIds.has(record.id.toString()));
      console.log(`ℹ️ Found ${newRecords.length} new records to process out of ${records.length}`);

      for (const record of newRecords) {
        console.log(`🔍 Processing record ${record.id}`);
        if (!record.combined_embeddings) {
          console.log(`ℹ️ No embedding found for record ${record.id}, generating new embedding`);
          await this.updateEmbeddingInXano(record);
        } else if (typeof record.combined_embeddings === "string") {
          try {
            console.log(`ℹ️ Parsing existing embedding for record ${record.id}`);
            record.combined_embeddings = JSON.parse(record.combined_embeddings);
          } catch (e) {
            console.error(`❌ Failed to parse embedding for record ${record.id}`);
            await this.logError(record.id, "Failed to parse combined_embeddings");
            record.combined_embeddings = [];
          }
        }
      }

      if (newRecords.length > 0) {
        console.log(`🗄️ Mapping ${newRecords.length} new records to Elasticsearch format`);
        const mapped = newRecords.map(this.mapToESFormat);
        await this.bulkInsertToES(mapped);
      } else {
        console.log("ℹ️ No new records to insert for this page");
      }

      this.lastProcessedPage = page;
      this.lastProcessedRecordId = Math.max(...records.map(r => parseInt(r.id)));
      console.log(`📌 Updating checkpoint: page ${this.lastProcessedPage}, last record ID ${this.lastProcessedRecordId}`);
      await this.saveCheckpoint();
    }

    console.log("\n✅ All records processed successfully");
  }
}

new XanoToES().run().catch((err) => console.error("❌ Fatal error in sync process:", err));