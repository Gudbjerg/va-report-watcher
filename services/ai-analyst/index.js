/**
 * services/ai-analyst/index.js
 * Minimal AI Analyst skeleton. Provides stubs for ingestion and query.
 * Replace the internals with your model provider of choice (OpenAI/Anthropic).
 */
async function ingestDocument(doc) {
    // doc: { id, text, metadata }
    // TODO: compute embeddings and store into vector store (Supabase or external)
    return { id: doc.id || null, status: 'ingested', summary: null };
}

async function summarizeDocument(doc) {
    // TODO: call a model to summarize
    return `Summary (mock): ${String(doc.text || '').slice(0, 200)}`;
}

async function queryAnalyst(query, opts = {}) {
    // TODO: use embedding+LLM retrieval to answer queries
    return { answer: `Mock answer to: ${query}`, source: null };
}

module.exports = { ingestDocument, summarizeDocument, queryAnalyst };
