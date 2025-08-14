require("dotenv").config();
const axios = require("axios");
const fs = require("fs/promises");
const path = require("path");
const { Client } = require("@elastic/elasticsearch");

class XanoToES {
  constructor() {
    this.config = {
      xanoApiKey: process.env.XANO_API_KEY,
      workspaceId: process.env.XANO_WORKSPACE_ID,
      metaBaseURL: process.env.XANO_META_BASE_URL,
      esUrl: process.env.ELASTICSEARCH_URL ,
      esIndex: process.env.ELASTICSEARCH_INDEX ,
      recordsPerPage: 2000,
      tableId: 106,
    };

    this.checkpointFile = path.join(__dirname, "checkpoint.json");
    this.errorFile = path.join(__dirname, "errors.json");
    this.lastProcessedPage = 0;
    this.lastProcessedRecordId = 0;
    this.failedRecords = [];

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
    { index: { _index: this.config.esIndex } },  // Let ES auto-generate _id
    doc,
  ]);

  try {
    console.log(`🚀 Sending bulk insert to Elasticsearch, index: ${this.config.esIndex}`);
    const response = await this.es.bulk({ body });
    
    if (response.body?.errors) {
      console.warn("⚠️ Some records failed to index in Elasticsearch");
      
      response.body.items.forEach((item, index) => {
        if (item.index?.error) {
          console.error(`❌ Elasticsearch error for record at index ${index}:`, item.index.error.reason);
          this.logError(item.index._id || `unknown-${index}`, `Elasticsearch Error: ${item.index.error.reason}`);
        }
      });

      console.error("🔴 Full Elasticsearch response for bulk insert:", JSON.stringify(response.body, null, 2));
    } else {
      console.log(`✅ Successfully inserted ${records.length} records to Elasticsearch`);
    }
  } catch (error) {
    console.error("❌ Bulk insert to Elasticsearch failed:", error.message);
    console.error("🔴 Full error details:", error.response ? error.response.data : error);
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

      // Bulk insert new records
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

// async run() {
//   console.log("🏁 Starting Xano to Elasticsearch sync process");
//   await this.loadCheckpoint();

//   let page = this.lastProcessedPage + 1;

//   while (true) {
//     console.log(`\n📄 Processing page ${page}`);
//     const records = await this.fetchRecords(page);

//     // Stop when Xano returns nothing
//     if (!records || records.length === 0) {
//       console.log("✅ No more records. Stopping.");
//       break;
//     }

//     // Check for existing records
//     const recordIds = records.map(r => String(r.id));
//     const existingIds = await this.checkExistingRecords(recordIds);
//     const newRecords = records.filter(r => !existingIds.has(String(r.id)));
//     console.log(`ℹ️ New records this page: ${newRecords.length}/${records.length}`);

//     if (newRecords.length > 0) {
//       console.log(`🗄️ Mapping ${newRecords.length} records to Elasticsearch format`);
//       const mapped = newRecords.map(this.mapToESFormat.bind(this));
//       await this.bulkInsertToES(mapped);
//     } else {
//       console.log("ℹ️ Nothing new to insert for this page");
//     }

//     // Update checkpoint
//     this.lastProcessedPage = page;
//     this.lastProcessedRecordId = Math.max(...records.map(r => parseInt(r.id, 10)).filter(Number.isFinite));
//     console.log(`📌 Updating checkpoint: page ${this.lastProcessedPage}, last record ID ${this.lastProcessedRecordId}`);
//     await this.saveCheckpoint();

//     // If the page is not full, we've hit the end
//     if (records.length < this.config.recordsPerPage) {
//       console.log("✅ Last page reached (short page). Stopping.");
//       break;
//     }

//     page += 1;
//   }

//   console.log("\n✅ All records processed successfully");
// }

}

new XanoToES().run().catch((err) => console.error("❌ Fatal error in sync process:", err));
