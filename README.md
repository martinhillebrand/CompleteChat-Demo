# CompleteChat - Teradata LLM Integration

A Teradata table operator that enables SQL-based integration with OpenAI-compatible inference servers. 
CompleteChat leverages Teradata's Massively Parallel Processing (MPP) architecture to perform LLM inference at scale, 
with each AMP making parallel API calls to process its data partition. This implementation represents **Design Pattern 3** for working with Language Models in Teradata—calling external inference servers directly from SQL queries to seamlessly integrate AI capabilities into ETL workflows without moving data outside the database.

## Getting Started

### 1. Get a Free Teradata Environment
- Visit [ClearScape Analytics Experience](https://clearscape.teradata.com/)
- Sign up for a free account that includes:
  - Jupyter notebook environment
  - Teradata Vantage database instance
- Clone this repository 

### 2. Get an OpenAI API Key
- Go to [OpenAI Platform](https://platform.openai.com/)
- Sign up or log in to your account
- Navigate to API Keys section
- Create a new API key
- Be sure you have some balance (It'll only consume very little, but you need to have 1 USD or so)

### 3. Run
Run the demo notebook.
