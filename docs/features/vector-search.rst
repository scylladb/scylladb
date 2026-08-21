=================================
Vector Search in ScyllaDB
=================================

.. note::

    This feature is currently available only in `ScyllaDB Cloud <https://cloud.docs.scylladb.com/>`_.

What Is Vector Search
-------------------------

Vector Search enables similarity-based queries over high-dimensional data,
such as text, images, audio, or user behavior. Instead of searching for exact
matches, it allows applications to find items that are semantically similar to
a given input.

To do this, Vector Search works on vector embeddings, which are numerical
representations of data that capture semantic meaning. This enables queries
such as:

* “Find documents similar to this paragraph”
* “Find products similar to what the user just viewed”
* “Find previous tickets related to this support request”

Rather than relying on exact values or keywords, Vector Search returns results
based on distance or similarity between vectors. This capability is
increasingly used in modern workloads such as AI-powered search, recommendation
systems, and retrieval-augmented generation (RAG).

Why Vector Search Matters
------------------------------------

Many applications already rely on ScyllaDB for high throughput, low and
predictable latency, and large-scale data storage.

Vector Search complements these strengths by enabling new classes of workloads,
including:

* Semantic search over text or documents
* Recommendations based on user or item similarity
* AI and ML applications, including RAG pipelines
* Anomaly and pattern detection

With Vector Search, ScyllaDB can serve as the similarity search backend for
AI-driven applications.

Availability
--------------

Vector Search is currently available only in ScyllaDB Cloud, the fully managed
ScyllaDB service.


👉 For details on using Vector Search, refer to the
`ScyllaDB Cloud documentation <https://cloud.docs.scylladb.com/stable/vector-search/index.html>`_.