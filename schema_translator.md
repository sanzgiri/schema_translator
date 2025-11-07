# **Schema Translator Requirements Document**

## **Project Overview**

Build an intelligent contract schema translation system that enables querying across multiple enterprise customers with heterogeneous database schemas using LLM-powered semantic understanding.

## **Core Problem**

* Multiple enterprise customers maintain different database schemas for contract data  
* Same business concepts represented differently across customers (structure, data types, semantics)  
* Need unified query interface that works across all customer schemas  
* Must handle semantic ambiguity (e.g., "contract\_value" meaning lifetime total vs. annual revenue)

## **Success Criteria**

1. User can ask natural language questions about contracts across all customers  
2. System correctly interprets semantic differences between customer schemas  
3. Results are normalized and harmonized for cross-customer comparison  
4. New customers can be onboarded with minimal manual configuration  
5. System learns and adapts from query patterns and errors

---

## **Architecture Components**

### **1\. Mock Data Layer**

**Purpose:** Generate realistic contract databases for 6 different customer schemas

**Requirements:**

* Create SQLite databases for 6 customers (A, B, C, D, E, F)  
* Each customer has unique schema characteristics:  
  * **Customer A:** Single table, DATE expiry, LIFETIME contract\_value  
  * **Customer B:** Normalized (3 tables: headers, status\_history, renewal\_schedule), LIFETIME value  
  * **Customer C:** Single table, DATE expiry, LIFETIME value, different column names  
  * **Customer D:** Single table, INTEGER days\_remaining instead of date, LIFETIME value  
  * **Customer E:** Single table, DATE expiry, LIFETIME contract\_value with explicit duration  
  * **Customer F:** Single table, DATE expiry, ANNUAL contract\_value (ARR)

**Data Requirements:**

* 50 contracts per customer  
* Fields: contract ID/name, status, expiration (various formats), value, industry/sector, customer name, start date  
* Realistic value ranges: $100K-$5M lifetime, $100K-$2M annual  
* Mix of expiration dates: some expired (-60 days), some upcoming (0-365 days)  
* Industry variety: Technology, Healthcare, Finance, Manufacturing, Retail (with variations in naming)

**Deliverable:**

* `schema_translator/mock_data.py` \- MockDataGenerator class  
* `databases/` directory with 6 SQLite .db files  
* Script to regenerate all databases

---

### **2\. Data Models**

**Purpose:** Define typed data structures for the entire system

**Requirements:**

* **SchemaColumn:** name, data\_type, semantic\_meaning, transformations, sample\_values  
* **SchemaTable:** name, columns, relationships  
* **CustomerSchema:** customer\_id, customer\_name, tables, semantic\_notes, last\_analyzed  
* **SemanticConcept:** concept\_id, concept\_name, description, customer\_mappings  
* **QueryFilter:** concept, operator, value, semantic\_note  
* **SemanticQueryPlan:** intent, filters, aggregations, projections  
* **QueryResult:** customer\_id, data, sql\_executed, execution\_time\_ms  
* **HarmonizedResult:** results, total\_count, customers\_queried

**Deliverable:**

* `schema_translator/models.py` using Pydantic BaseModel  
* Full type hints and validation  
* JSON serialization support

---

### **3\. Configuration Management**

**Purpose:** Centralized configuration for API keys, paths, LLM settings

**Requirements:**

* Load from environment variables and .env file  
* ANTHROPIC\_API\_KEY (required)  
* Model name: claude-sonnet-4-20250514  
* Database directory path  
* Knowledge graph storage path  
* LLM parameters: max\_tokens, temperature  
* Validation on startup

**Deliverable:**

* `schema_translator/config.py` \- Config class  
* `.env.example` file  
* Configuration validation method

---

### **4\. Knowledge Graph**

**Purpose:** Store semantic relationships between schemas and concepts

**Requirements:**

#### **Core Concepts to Model:**

1. **contract\_expiration** \- When contract ends (DATE or INTEGER days)  
2. **contract\_value** \- Monetary value (LIFETIME vs ANNUAL semantics)  
3. **contract\_status** \- Current state (single table vs history table)  
4. **industry\_sector** \- Business vertical (various naming conventions)  
5. **contract\_identifier** \- Primary key (ID vs name)  
6. **customer\_name** \- Client organization  
7. **contract\_start** \- Contract begin date

#### **Customer Mappings:**

For each customer, map each concept to:

* Table name  
* Column name  
* Data type  
* Semantic type (e.g., LIFETIME\_TOTAL, ANNUAL\_RECURRING\_REVENUE)  
* Required transformation (if any)  
* Join requirements (for multi-table schemas)

#### **Transformation Rules:**

* **days\_remaining → DATE:** `DATE_ADD(NOW(), INTERVAL days_remaining DAY)`  
* **DATE → days\_remaining:** `DATEDIFF(expiry_date, NOW())`  
* **ANNUAL → LIFETIME:** `contract_value * term_years`  
* **LIFETIME → ANNUAL:** `contract_value / term_years`

**Deliverable:**

* `schema_translator/knowledge_graph.py` \- SchemaKnowledgeGraph class  
* Methods: add\_concept(), add\_customer\_mapping(), add\_transformation(), get\_mapping(), save(), load()  
* JSON persistence  
* NetworkX graph structure for relationship traversal  
* Pre-populated with all 6 customer mappings

---

### **5\. LLM Schema Analyzer Agent**

**Purpose:** Automatically analyze new customer schemas and propose mappings

**Requirements:**

#### **Core Functionality:**

1. **Analyze Schema:**

   * Input: Customer database connection, customer\_id  
   * Extract table structures, column names, data types  
   * Query sample data from each column  
   * Identify primary keys and foreign keys  
2. **Semantic Analysis:**

   * Use LLM to interpret column names and sample values  
   * Determine semantic meaning (is "contract\_value" lifetime or annual?)  
   * Identify calculation needs (days\_remaining vs date conversion)  
   * Detect normalization patterns (single vs multi-table)  
3. **Mapping Proposal:**

   * Generate proposed mappings to canonical concepts  
   * Suggest transformation logic  
   * Flag ambiguities requiring human confirmation  
   * Provide confidence scores  
4. **Validation:**

   * Test proposed mappings with sample queries  
   * Verify JOIN logic for multi-table schemas  
   * Confirm value ranges are reasonable

**LLM Prompts Required:**

* Schema interpretation prompt  
* Semantic disambiguation prompt (for ambiguous columns)  
* Transformation generation prompt  
* Validation prompt

**Deliverable:**

* `schema_translator/agents/schema_analyzer.py` \- SchemaAnalyzerAgent class  
* Methods: analyze\_schema(), propose\_mappings(), validate\_mappings()  
* Integration with Anthropic API  
* Structured output parsing

---

### **6\. Query Understanding Agent**

**Purpose:** Convert natural language queries to Semantic Query Plans

**Requirements:**

#### **Input Processing:**

* Accept natural language questions about contracts  
* Parse intent (find, count, aggregate, compare)  
* Extract filters (time ranges, value thresholds, industry, status)  
* Identify aggregations (sum, count, average)  
* Determine projections (which fields to return)

#### **Semantic Query Plan Generation:**

Convert to schema-independent intermediate representation:

{  
  "intent": "find\_contracts",  
  "filters": \[  
    {  
      "concept": "contract\_expiration",  
      "operator": "within\_next\_days",  
      "value": 30,  
      "semantic\_note": "expiration may be date or days\_remaining"  
    },  
    {  
      "concept": "contract\_value",  
      "operator": "greater\_than",  
      "value": 500000,  
      "semantic\_note": "need to normalize to lifetime total"  
    },  
    {  
      "concept": "industry\_sector",  
      "operator": "equals",  
      "value": "Technology"  
    }  
  \],  
  "projections": \["contract\_identifier", "contract\_value", "contract\_expiration", "customer\_name"\],  
  "aggregations": null  
}

#### **Operator Mapping:**

* Natural language → standardized operators  
* "expiring soon", "due in next X days" → within\_next\_days  
* "over $X", "worth more than" → greater\_than  
* "in Q1", "between dates" → date\_range  
* "technology sector", "tech companies" → equals (with normalization)

**LLM Prompts Required:**

* Query understanding prompt  
* Ambiguity resolution prompt  
* Context-aware concept extraction

**Deliverable:**

* `schema_translator/agents/query_understanding.py` \- QueryUnderstandingAgent class  
* Methods: parse\_query(), create\_semantic\_plan()  
* Natural language processing with LLM  
* Structured SemanticQueryPlan output

---

### **7\. Query Compiler**

**Purpose:** Compile Semantic Query Plans to customer-specific SQL

**Requirements:**

#### **Core Compilation:**

1. **Concept Resolution:**

   * For each concept in plan, lookup customer mapping in knowledge graph  
   * Retrieve table, column, type, semantic type  
   * Identify required transformations  
2. **SQL Generation:**

   * Build SELECT clause with projections  
   * Build FROM clause (single table or JOINs)  
   * Build WHERE clause with filters  
   * Apply transformations in WHERE/SELECT as needed  
   * Build GROUP BY, HAVING for aggregations  
3. **Schema-Specific Handling:**

   * **Single table (A, C, D, E, F):** Direct SELECT  
   * **Multi-table (B):** Auto-generate JOINs  
   * **Date fields (A, C, E, F):** Direct date comparisons  
   * **Days remaining (D):** Convert to date calculations  
   * **Lifetime value (A, C, D, E):** Direct value comparison  
   * **Annual value (F):** Multiply by term\_years for lifetime equivalent  
4. **Industry Normalization:**

   * Map "Technology" → customer-specific terms (Tech, Information Technology)  
   * Handle semantic equivalence

#### **Example Compilations:**

**Query:** "Contracts expiring in next 30 days worth over $500K"

**Customer A SQL:**

SELECT contract\_id, contract\_name, contract\_value, expiry\_date, customer\_name  
FROM contracts  
WHERE expiry\_date BETWEEN CURRENT\_DATE AND DATE(CURRENT\_DATE, '+30 days')  
  AND contract\_value \> 500000

**Customer B SQL:**

SELECT h.id, h.contract\_name, h.contract\_value, r.renewal\_date, h.client\_name  
FROM contract\_headers h  
JOIN renewal\_schedule r ON h.id \= r.contract\_id  
JOIN contract\_status\_history s ON h.id \= s.contract\_id  
WHERE r.renewal\_date BETWEEN CURRENT\_DATE AND DATE(CURRENT\_DATE, '+30 days')  
  AND h.contract\_value \> 500000  
  AND s.status\_date \= (  
    SELECT MAX(status\_date)   
    FROM contract\_status\_history   
    WHERE contract\_id \= h.id  
  )

**Customer D SQL:**

SELECT contract\_id, contract\_title, contract\_value,   
       DATE(CURRENT\_DATE, '+' || days\_remaining || ' days') as expiry\_date,  
       customer\_org  
FROM contracts  
WHERE days\_remaining BETWEEN 0 AND 30  
  AND contract\_value \> 500000

**Customer F SQL:**

SELECT contract\_id, name,   
       (contract\_value \* term\_years) as lifetime\_value,  
       expiration\_date, account  
FROM contracts  
WHERE expiration\_date BETWEEN CURRENT\_DATE AND DATE(CURRENT\_DATE, '+30 days')  
  AND (contract\_value \* term\_years) \> 500000

**Deliverable:**

* `schema_translator/query_compiler.py` \- QueryCompiler class  
* Methods: compile\_for\_customer(), generate\_select(), generate\_where(), generate\_joins()  
* SQL injection prevention  
* Query validation

---

### **8\. Database Executor**

**Purpose:** Execute customer-specific SQL queries and collect results

**Requirements:**

#### **Execution:**

* Connect to customer SQLite databases  
* Execute compiled SQL queries  
* Measure execution time  
* Handle database errors gracefully  
* Return structured QueryResult objects

#### **Multi-Customer Execution:**

* Execute queries across multiple customers in parallel (optional: can be sequential for MVP)  
* Collect results from all customers  
* Tag results with customer\_id  
* Include executed SQL for debugging

#### **Error Handling:**

* Catch SQL errors (invalid column, syntax, etc.)  
* Return partial results if some customers succeed  
* Log errors with context  
* Provide actionable error messages

**Deliverable:**

* `schema_translator/database_executor.py` \- DatabaseExecutor class  
* Methods: execute\_query(), execute\_for\_customer(), execute\_all\_customers()  
* Connection pooling  
* Result aggregation

---

### **9\. Result Harmonizer**

**Purpose:** Normalize and harmonize results from different customer schemas

**Requirements:**

#### **Normalization:**

1. **Value Normalization:**

   * Convert all contract values to same semantic unit (lifetime total)  
   * Customer F: multiply annual by term\_years  
   * Add metadata indicating original vs calculated  
2. **Date Normalization:**

   * Convert all expiration representations to ISO date format  
   * Customer D: calculate actual date from days\_remaining  
   * Ensure consistent timezone handling  
3. **Field Name Normalization:**

   * Map customer-specific field names to canonical names  
   * contract\_id/id → contract\_identifier  
   * customer\_name/client\_name/account → customer\_name  
   * expiry\_date/expiration\_date/renewal\_date → expiration\_date  
4. **Industry Name Normalization:**

   * Map variations to canonical terms  
   * "Tech", "Technology", "Information Technology" → "Technology"  
   * Maintain consistent taxonomy

#### **Result Formatting:**

* Unified schema for display  
* Include source customer  
* Include both original and normalized values where applicable  
* Sort by specified criteria (e.g., expiration date, value)  
* Support pagination

**Deliverable:**

* `schema_translator/result_harmonizer.py` \- ResultHarmonizer class  
* Methods: harmonize\_results(), normalize\_value(), normalize\_date(), normalize\_industry()  
* Configurable normalization rules

---

### **10\. Chat Orchestrator**

**Purpose:** Coordinate the entire query flow from natural language to harmonized results

**Requirements:**

#### **Workflow:**

1. Receive natural language query from user  
2. Optionally filter by specific customer(s) or query all  
3. Call QueryUnderstandingAgent → SemanticQueryPlan  
4. For each customer:  
   * Call QueryCompiler → customer-specific SQL  
   * Call DatabaseExecutor → QueryResult  
5. Call ResultHarmonizer → HarmonizedResult  
6. Format for display  
7. Return to user interface

#### **Additional Features:**

* Query validation before execution  
* Caching for repeated queries  
* Query history tracking  
* Performance metrics (end-to-end latency)  
* Error aggregation and reporting

#### **Conversation Context:**

* Maintain conversation state for follow-up questions  
* Reference previous queries ("show me more like this")  
* Clarification handling ("Did you mean X or Y?")

**Deliverable:**

* `schema_translator/orchestrator.py` \- ChatOrchestrator class  
* Methods: process\_query(), validate\_query(), execute\_pipeline()  
* Integration with all agents and components  
* Comprehensive logging

---

### **11\. Chat Interface (Chainlit)**

**Purpose:** Web-based chat UI for interacting with the Schema Translator

**Requirements:**

#### **UI Features:**

1. **Chat Interface:**

   * Natural language input box  
   * Streaming responses  
   * Display harmonized results in tables  
   * Show per-customer breakdowns  
   * Display executed SQL (collapsible/expandable)  
   * Error messages with retry options  
2. **Customer Selection:**

   * Dropdown or checkboxes to select specific customers  
   * "All customers" option (default)  
   * Show customer schema summaries on hover  
3. **Result Display:**

   * Table view with sortable columns  
   * Highlight normalized values  
   * Show original vs calculated values  
   * Export to CSV option  
   * Pagination for large result sets  
4. **Query Examples:**

   * Pre-populated example queries  
   * "Show me contracts expiring in next 30 days"  
   * "Find technology contracts over $1M"  
   * "What's the total value of contracts expiring in Q1?"  
   * "Compare contract values across customers"  
5. **Debug Mode:**

   * Toggle to show semantic query plan  
   * Show customer-specific SQL for each customer  
   * Show knowledge graph lookups  
   * Execution time per customer

#### **Chainlit-Specific:**

* Use @cl.on\_message for query handling  
* Use cl.Message for responses  
* Use cl.DataTable for results display (or custom HTML)  
* Use cl.Action for interactive buttons  
* Session management for conversation context

**Deliverable:**

* `app.py` \- Main Chainlit application  
* Integration with ChatOrchestrator  
* Responsive UI with tables and formatting  
* Error handling and user feedback

---

### **12\. Learning and Feedback Loop**

**Purpose:** Improve system over time based on query results and user feedback

**Requirements:**

#### **Feedback Collection:**

* Capture user corrections ("This result is wrong")  
* Track query failures and errors  
* Log semantic ambiguities that required clarification  
* Record query patterns and frequency

#### **Learning Mechanisms:**

1. **Schema Drift Detection:**

   * Periodically check customer schemas for changes  
   * Detect new columns, modified types  
   * Flag when mappings may be stale  
2. **Transformation Refinement:**

   * When queries fail, analyze error  
   * Use LLM to propose fixes  
   * Update knowledge graph with successful fixes  
3. **Semantic Learning:**

   * Track which industry name mappings are used most  
   * Learn customer-specific terminology preferences  
   * Improve concept disambiguation over time  
4. **Query Pattern Analysis:**

   * Identify common query types  
   * Pre-generate optimized mappings for frequent patterns  
   * Suggest query completions

**Deliverable:**

* `schema_translator/learning/feedback_loop.py` \- FeedbackLoop class  
* `schema_translator/learning/schema_drift_detector.py` \- SchemaDriftDetector class  
* Methods: record\_feedback(), detect\_drift(), refine\_transformations()  
* Periodic background tasks

---

## **Implementation Phases**

### **Phase 1: Foundation (Week 1\)**

**Goal:** Core data structures and mock data

**Tasks:**

1. Set up project structure  
2. Implement configuration management  
3. Define all Pydantic models  
4. Generate mock data for all 6 customers  
5. Test database creation and data access

**Validation:**

* All 6 SQLite databases created with 50 contracts each  
* Can query each database directly  
* Models serialize to/from JSON correctly

---

### **Phase 2: Knowledge Graph (Week 1-2)**

**Goal:** Build and populate the knowledge graph

**Tasks:**

1. Implement SchemaKnowledgeGraph class  
2. Define 7 core concepts with aliases  
3. Create mappings for all 6 customers  
4. Implement transformation rules  
5. Add JSON persistence  
6. Write unit tests

**Validation:**

* Knowledge graph loads and saves correctly  
* Can query concept mappings for each customer  
* Transformations are correctly defined  
* No missing mappings for any customer

---

### **Phase 3: Query Compiler & Executor (Week 2\)**

**Goal:** Generate and execute customer-specific SQL

**Tasks:**

1. Implement QueryCompiler  
2. Handle single-table schemas (A, C, D, E, F)  
3. Handle multi-table schema (B)  
4. Implement date/days\_remaining conversions  
5. Implement lifetime/annual value conversions  
6. Implement DatabaseExecutor  
7. Write extensive unit tests with real queries

**Validation:**

* Same semantic query compiles to different SQL for each customer  
* All compiled queries execute successfully  
* Results are returned in QueryResult format  
* Customer D date calculations are correct  
* Customer F value normalization is correct  
* Customer B joins work correctly

---

### **Phase 4: LLM Agents (Week 2-3)**

**Goal:** Implement intelligent query understanding and schema analysis

**Tasks:**

1. Set up Anthropic API integration  
2. Implement QueryUnderstandingAgent  
3. Test natural language → SemanticQueryPlan conversion  
4. Implement SchemaAnalyzerAgent  
5. Test auto-analysis on a mock "Customer G"  
6. Implement prompt engineering and few-shot examples

**Validation:**

* Natural language queries correctly parsed  
* Semantic query plans are accurate  
* Schema analyzer correctly interprets new schemas  
* LLM proposes reasonable mappings  
* Ambiguity detection works (flags when uncertain)

---

### **Phase 5: Result Harmonization (Week 3\)**

**Goal:** Normalize and present unified results

**Tasks:**

1. Implement ResultHarmonizer  
2. Implement value normalization (annual → lifetime)  
3. Implement date normalization (days → date)  
4. Implement field name mapping  
5. Implement industry name normalization  
6. Test with multi-customer result sets

**Validation:**

* Customer F annual values correctly converted  
* Customer D dates correctly calculated  
* Field names unified across customers  
* Industry names normalized  
* Results sortable and filterable

---

### **Phase 6: Orchestration (Week 3\)**

**Goal:** Connect all components into cohesive pipeline

**Tasks:**

1. Implement ChatOrchestrator  
2. Integrate all agents and components  
3. Implement error handling and logging  
4. Implement query validation  
5. Add performance monitoring  
6. Write integration tests

**Validation:**

* End-to-end query flow works  
* Errors handled gracefully  
* Can query all customers simultaneously  
* Can filter by specific customer(s)  
* Query history maintained  
* Performance acceptable (\<5s for simple queries)

---

### **Phase 7: UI Implementation (Week 4\)**

**Goal:** Build user-facing chat interface

**Tasks:**

1. Set up Chainlit application  
2. Implement chat message handling  
3. Implement result table display  
4. Add customer selection controls  
5. Add example queries  
6. Implement debug mode  
7. Add error handling and user feedback  
8. Style and polish UI

**Validation:**

* Chat interface responsive and intuitive  
* Results display in readable tables  
* Customer filtering works  
* Example queries load and execute  
* Debug mode shows SQL and query plans  
* Errors displayed clearly with retry options

---

### **Phase 8: Learning & Polish (Week 4\)**

**Goal:** Add learning capabilities and final refinements

**Tasks:**

1. Implement FeedbackLoop  
2. Implement SchemaDriftDetector  
3. Add query failure analysis  
4. Add transformation refinement  
5. Comprehensive testing across all components  
6. Documentation and deployment guide  
7. Performance optimization

**Validation:**

* System learns from errors  
* Schema drift detected  
* Query failures analyzed and fixed  
* End-to-end system robust and performant  
* Documentation complete

---

## **File Structure**

schema\_translator/  
├── README.md  
├── requirements.txt  
├── .env.example  
├── .gitignore  
│  
├── databases/                      \# SQLite databases  
│   ├── customer\_a.db  
│   ├── customer\_b.db  
│   ├── customer\_c.db  
│   ├── customer\_d.db  
│   ├── customer\_e.db  
│   └── customer\_f.db  
│  
├── schema\_translator/  
│   ├── \_\_init\_\_.py  
│   ├── config.py                   \# Configuration management  
│   ├── models.py                   \# Pydantic data models  
│   ├── mock\_data.py                \# Mock data generation  
│   ├── knowledge\_graph.py          \# Schema knowledge graph  
│   ├── query\_compiler.py           \# SQL generation  
│   ├── database\_executor.py        \# Query execution  
│   ├── result\_harmonizer.py        \# Result normalization  
│   ├── orchestrator.py             \# Main pipeline orchestrator  
│   │  
│   ├── agents/  
│   │   ├── \_\_init\_\_.py  
│   │   ├── schema\_analyzer.py      \# LLM schema analysis  
│   │   └── query\_understanding.py  \# NLP → Semantic query plan  
│   │  
│   └── learning/  
│       ├── \_\_init\_\_.py  
│       ├── feedback\_loop.py        \# User feedback processing  
│       └── schema\_drift\_detector.py \# Schema change detection  
│  
├── tests/  
│   ├── \_\_init\_\_.py  
│   ├── test\_models.py  
│   ├── test\_knowledge\_graph.py  
│   ├── test\_query\_compiler.py  
│   ├── test\_database\_executor.py  
│   ├── test\_agents.py  
│   └── test\_integration.py  
│  
├── app.py                          \# Chainlit application  
├── knowledge\_graph.json            \# Persisted knowledge graph  
└── query\_history.json              \# Query logs

---

## **Testing Requirements**

### **Unit Tests**

* All data models validate correctly  
* Knowledge graph operations (add, query, save, load)  
* Query compiler for each customer schema type  
* Database executor with mock databases  
* Result harmonizer with various data types  
* Each agent independently

### **Integration Tests**

* End-to-end query flow (NL → results)  
* Multi-customer queries  
* Error scenarios (invalid SQL, missing mappings)  
* Schema analysis on mock new customer  
* Learning loop with feedback

### **Performance Tests**

* Query latency (target: \<5s for simple queries across 6 customers)  
* Knowledge graph lookup speed  
* LLM API call optimization (minimize calls)  
* Database query optimization

---

## **Non-Functional Requirements**

### **Security**

* SQL injection prevention (parameterized queries)  
* API key protection (environment variables)  
* Input validation on all user queries

### **Scalability**

* Knowledge graph should support 50+ customers  
* Query execution should parallelize where possible  
* Results pagination for large datasets

### **Maintainability**

* Comprehensive docstrings  
* Type hints throughout  
* Clear separation of concerns  
* Modular architecture for easy updates

### **Observability**

* Logging at all pipeline stages  
* Query performance metrics  
* Error tracking with context  
* Debug mode for development

---

## **Example Queries to Support**

1. "Show me contracts expiring in the next 30 days"  
2. "Find all technology contracts worth over $1 million"  
3. "What's the total value of contracts expiring in Q1 2025?"  
4. "Show me inactive or expired contracts"  
5. "Which customers have contracts expiring soon?" (group by customer)  
6. "Compare contract values between healthcare and technology sectors"  
7. "Find contracts for Customer A that expire before February"  
8. "What's the average contract value across all customers?"  
9. "Show me the top 10 highest value contracts"  
10. "Find contracts that started in 2023 and are still active"

---

## **Deliverables Summary**

1. ✅ Working system with all components integrated  
2. ✅ 6 customer mock databases with realistic data  
3. ✅ Knowledge graph with all mappings  
4. ✅ LLM-powered query understanding  
5. ✅ Customer-specific SQL generation  
6. ✅ Result harmonization and normalization  
7. ✅ Chainlit chat interface  
8. ✅ Comprehensive test suite  
9. ✅ Documentation and README  
10. ✅ Deployment instructions

---

## **Success Metrics**

1. **Accuracy:** \>95% of test queries return correct results  
2. **Coverage:** All 10 example queries work across all 6 customers  
3. **Performance:** \<5 seconds for simple queries, \<15 seconds for complex  
4. **Usability:** Non-technical users can successfully query data  
5. **Adaptability:** New customer onboarding takes \<1 hour

---

## **Tech Stack**

* **Language:** Python 3.10+  
* **LLM:** Anthropic Claude (claude-sonnet-4-20250514)  
* **Database:** SQLite  
* **UI Framework:** Chainlit  
* **Data Validation:** Pydantic  
* **Graph:** NetworkX  
* **Testing:** pytest  
* **Environment:** python-dotenv

---

## **Getting Started Instructions**

\# 1\. Clone repository  
git clone \<repo-url\>  
cd schema\_translator

\# 2\. Create virtual environment  
python \-m venv venv  
source venv/bin/activate  \# On Windows: venv\\Scripts\\activate

\# 3\. Install dependencies  
pip install \-r requirements.txt

\# 4\. Set up environment  
cp .env.example .env  
\# Edit .env and add ANTHROPIC\_API\_KEY

\# 5\. Generate mock data  
python \-m schema\_translator.mock\_data

\# 6\. Run tests  
pytest tests/

\# 7\. Start application  
chainlit run app.py

---

## **Notes for Claude Code**

* Build incrementally, validating each component before moving to next  
* Start with data layer (models, mock\_data, config) before logic  
* Test each customer schema type independently  
* Use real LLM calls early to validate prompts  
* Focus on Customer A, D, and F first (they demonstrate key differences)  
* Customer B can be added later (multi-table complexity)  
* Prioritize correctness over performance initially  
* Add comprehensive error messages for debugging  
* Use print/logging statements liberally during development

