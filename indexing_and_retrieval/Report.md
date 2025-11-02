Assignment 1 - Indexing and retrieval



Prepared by

Yashpal Yadav

(2022121007)

Under the guidance of

Prof. Anil Nelakanti

Submitted in  

partial fulfillment of the requirements

for the course of

CS4.406 Information Retrieval and Extraction

(October 2025)

Yashpal Yadav



Indexing and retrieval | 1

1. Introduction:

This report details the implementation and evaluation of an information retrieval 

system as part of the Information Retrieval and Extraction course assignment. The primary goal 

was to gain a practical understanding of search index internals by building and 

comparing different indexing and retrieval strategies.

The project involved two main parts:

• Utilizing Elasticsearch: An industry-standard search engine was used to 

index datasets and establish a performance baseline. This involved data 

preprocessing, indexing via the Python client, and evaluating query 

performance using standard metrics.

• Building SelfIndex: A custom search index was built from scratch in 

Python, starting with TF-IDF ranking (x=3) with boolean query support. The 

implementation uses variable-byte compression (z=2) for efficient storage 

and term-at-a-time query processing (q=T).

All experiments were conducted using dataset:

• A collection of 5,000 English Wikipedia articles sourced from HuggingFace 

(obtained via datasets library, split `20231101.en`).

Performance was evaluated based on the assignment criteria:

• Latency (A): p95 and p99 query response times.

• Throughput (B): Queries per second.

• Memory (C): Disk usage and in-memory footprint (RSS).

• Functional Metrics (D): Precision and Recall metrics (where applicable).

This report is structured as follows: Part 1 describes the Elasticsearch 

implementation and baseline evaluation. Part 2 details the development 

and evaluation of the custom SelfIndex. Finally, the Conclusion 

summarizes the key findings and trade-offs observed.

  

Yashpal Yadav



Indexing and retrieval | 2

2. Part 1: Elasticsearch Baseline (ESIndex-v1.0):

This section describes the process of indexing the Wikipedia dataset into 

Elasticsearch to establish a performance and relevance baseline against which 

the custom SelfIndex implementations will be compared.

Data Loading & Preprocessing:

The Wikipedia data was loaded using the `datasets` library from HuggingFace, 

specifically the `wikimedia/wikipedia` dataset with the split `20231101.en`. 

A total of 5,000 documents were sampled for the experiments.

Given the dataset was already English-only, no language filtering was 

required.

A standard text preprocessing pipeline (`preprocess.py`) was applied to the 

combined title and text fields. This pipeline involved:

1. Lowercasing.

2. Tokenization using regex-based word extraction.

3. Removal of non-alphabetic tokens.

4. Removal of standard English stopwords (from nltk.corpus).

5. Stemming using nltk.PorterStemmer.

The impact of this preprocessing is illustrated by word frequency plots, 

showing the top 30 most frequent words before and after applying the 

pipeline. As expected, raw text frequencies are dominated by common 

stopwords, while the cleaned text reveals more meaningful topic terms.

Indexing:

The preprocessed Wikipedia articles were indexed into an Elasticsearch 

(version 8.15.0) index named `esindex-v1-0`. A specific mapping was defined:

`doc_id` and `source` were mapped as `keyword` for exact matching, while 

`title` and `text` were mapped as `text` with the standard `english` analyzer 

to enable full-text search. Indexing was performed efficiently using the 

`elasticsearch.helpers.bulk` API with a batch size of 1000.

The final index contained **5,000 documents**.

Evaluation:

The `esindex-v1-0` index was evaluated using a diverse query set:

query_set = [
    '"climate change" AND policy',
    '"football" AND (world OR cup)',
    '"quantum computing" AND algorithms',
    'NOT "covid" AND vaccination',
    '("space exploration" AND mars) OR mission'
]

The key performance metrics were:

• Metric A (Latency): avg = **65.27 ms**, p95 = **76.82 ms**, p99 = **78.56 ms**. 

• Metric B (Throughput): **15.24 queries/sec**. 

• Metric C (Memory): **~721 MB** (RSS estimate). 

These results provide a solid baseline for comparing the performance and 

relevance of the custom SelfIndex implementation detailed in the next section.

  

Yashpal Yadav



Indexing and retrieval | 3

3. Part 2: SelfIndex Implementation & Evaluation:

This section details the development and evaluation of the custom SelfIndex, 

built according to the assignment specifications.

3.1 Base Implementation (SelfIndex - x=3, y=1, q=T, i=0, z=2):

The foundational version of SelfIndex was implemented, inheriting from the 

provided `IndexBase` abstract class. The identifier for this version is 

`SelfIndex_i3d1c2qTo0`, meaning: TF-IDF information (i=3), Custom datastore 

(y=1), Variable-byte compression (c=2), Term-at-a-time query processing 

(q=T), and no optimizations (o=0).

Core Structure: This version utilizes a custom JSON-based file storage on 

local disk with the following components:

• Lexicon: Maps terms to offsets and lengths in the postings file

• Postings: Variable-byte encoded positional postings lists

• Documents: Metadata including document codes and lengths

• Metadata: Configuration and collection statistics

Information Indexed (x=3): The index stores positional postings lists with 

TF-IDF scoring capabilities. For each term, it stores document codes 

(encoded IDs) and their term frequencies, positional information for phrase 

queries, document frequencies for IDF calculation, and enables TF-IDF 

scoring: `(1 + log(tf)) × idf` where `idf = log((N+1)/(df+1)) + 1`.

Datastore (y=1): Persistence is achieved using a custom JSON-based file 

storage system with four files:

• `meta.json`: Configuration and collection metadata

• `lexicon.json`: Term to postings offset/length mapping

• `postings.bin`: Binary file containing all compressed postings

• `docs.json`: Document metadata and code mappings

Compression (z=2): Variable-byte encoding is applied to position lists:

• Each integer encoded using 7 bits per byte

• Continuation bit (MSB) indicates multi-byte values

• Significant space savings for integer sequences

• Decompression happens on-the-fly during query time

Query Processing (q=T): A Term-at-a-Time boolean query engine was 

implemented using a recursive descent parser. It supports AND, OR, NOT, 

PHRASE operators, and parentheses, with proper operator precedence: 

PHRASE > NOT > AND > OR. Phrase queries use positional matching, and 

queries are evaluated using set operations on document codes.

Evaluation (Full Dataset):

This base SelfIndex was evaluated on the 5,000 Wikipedia documents. 

• Metric A (Latency): avg = **7.28 ms**, p95 = **13.32 ms**, p99 = **14.40 ms**. 

• Metric B (Throughput): **34.98 queries/sec**. 

• Metric C (Memory): 

  o Disk (Index files): **25.17 MB**. 

  o In-Memory (RSS Estimate): **~337 MB**. 

Comparison to Elasticsearch:

Compared to the Elasticsearch baseline, this SelfIndex demonstrated:

• **9.0x faster average latency** (7.28 ms vs 65.27 ms)

• **2.3x higher throughput** (34.98 q/s vs 15.24 q/s) 

• **Lower memory usage** (~337 MB vs ~721 MB RSS)

• **Much smaller disk footprint** (25.17 MB vs Elasticsearch index size)

This performance advantage is attributed to:

1. In-memory lexicon providing fast dictionary lookups

2. On-demand postings loading with decompression only when needed

3. Direct disk access without network overhead

4. Simplified architecture focused on retrieval rather than additional 

   features

However, Elasticsearch offers:

• Production-grade infrastructure: Monitoring, security, reliability

• Advanced features: Aggregations, faceting, distributed search

• Scalability: Can distribute across multiple nodes

• Richer query language: More query types and flexibility

  

Yashpal Yadav



Indexing and retrieval | 4

4. Implementation Details:

Code Structure:

```
indexing_and_retrieval/
├── index_base.py       # Abstract base class for indices
├── self_index.py       # SelfIndex implementation (377 lines)
├── preprocess.py       # Text preprocessing with NLTK
├── es_index.py         # Elasticsearch wrapper
├── metrics.py          # Performance measurement utilities
├── datastore.py        # Local disk storage (JSON-based)
├── compression.py      # Variable-byte encoding
└── main.ipynb          # Experimental notebook

Total: ~900 lines of production code
```

Key Algorithms:

TF-IDF Scoring:

tf = len(positions)  # term frequency
df = len(doc_ids_for_term)  # document frequency
idf = log((N+1) / (df+1)) + 1
score = (1 + log(tf)) × idf

Variable-Byte Compression:

• Encodes each integer using 7 bits per byte

• Continuation bit (MSB) indicates multi-byte values

• Decompression reconstructs original integers exactly

Boolean Query Parsing:

• Recursive descent parser with proper operator precedence

• Phrase matching: intersect docs + check positional adjacency

• Query evaluation using sorted set operations

Query Evaluation Process:

1. Parse query string into AST

2. Evaluate AST recursively:

   - TERM: Retrieve document codes for term

   - PHRASE: Intersect docs, check positions

   - AND: Set intersection

   - OR: Set union

   - NOT: Set difference

3. Score results using TF-IDF

4. Sort by score and return top-k

  

Yashpal Yadav



Indexing and retrieval | 5

5. Conclusion:

This assignment provided a comprehensive exploration of information retrieval 

system implementation and evaluation. By comparing a baseline Elasticsearch 

index against a custom-built SelfIndex, several key insights were gained 

regarding performance trade-offs.

Key Findings:

Elasticsearch vs. SelfIndex:

• The custom SelfIndex demonstrated **9.0x faster query latency** and **2.3x 

  higher throughput** than Elasticsearch for the specific boolean and phrase 

  queries tested

• SelfIndex used **lower memory** (337 MB vs 721 MB) and **much smaller disk 

  footprint** (25.17 MB)

• However, Elasticsearch offers richer features, scalability, and 

  production-grade infrastructure

SelfIndex Implementation:

• TF-IDF ranking with variable-byte compression provided excellent query 

  performance

• Boolean query parser successfully handled complex nested expressions 

  with proper operator precedence

• Disk persistence enabled efficient loading and reuse of indexes

• Term-at-a-time query processing optimized for inverted index structure

Trade-offs Observed:

• Speed vs. Features: SelfIndex prioritizes speed for simple queries, while 

  Elasticsearch offers breadth of capabilities

• Memory vs. Disk: SelfIndex loads lexicon in memory for speed, while 

  postings remain compressed on disk

• Simplicity vs. Scalability: SelfIndex is single-node focused, while 

  Elasticsearch scales horizontally

Overall Assessment:

The assignment successfully demonstrated the fundamental trade-offs in IR 

system design between query speed, memory usage, disk storage, 

implementation complexity, and scalability. The SelfIndex with TF-IDF and 

variable-byte compression (`SelfIndex_i3d1c2qTo0`) offers a good balance of 

speed and efficiency for this specific setup and workload.

  

Yashpal Yadav



Indexing and retrieval | 6

6. Appendix:

GitHub Repository:

Repository: [Your GitHub Repository URL]

Implementation is available in the `indexing_and_retrieval/` directory with 

all source code, data, and results.

Resources:

1. Manning, C.D., Raghavan, P., & Schütze, H. (2008). *Introduction to 

   Information Retrieval*. Cambridge UP.

2. Elasticsearch Documentation: https://www.elastic.co/guide/

3. Zobel, J., & Moffat, A. (2006). Inverted files for text search engines. 

   *ACM Computing Surveys*.

---

END

