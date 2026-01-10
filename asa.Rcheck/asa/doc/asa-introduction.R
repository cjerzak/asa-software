## ----setup, include = FALSE---------------------------------------------------
knitr::opts_chunk$set(
  collapse = TRUE,
  comment = "#>",
  eval = FALSE
)

## ----install-package----------------------------------------------------------
# # Install from GitHub
# devtools::install_github("cjerzak/asa-software/asa")
# 
# # Or install locally from the repository
# devtools::install("path/to/asa-software/asa")

## ----build-backend------------------------------------------------------------
# library(asa)
# 
# # Create conda environment with all Python dependencies
# build_backend(conda_env = "asa_env")

## ----api-keys-----------------------------------------------------------------
# # For OpenAI (recommended for best results)
# Sys.setenv(OPENAI_API_KEY = "your-openai-api-key")
# 
# # For Groq (fast inference, free tier available)
# Sys.setenv(GROQ_API_KEY = "your-groq-api-key")
# 
# # For xAI
# Sys.setenv(XAI_API_KEY = "your-xai-api-key")

## ----basic-query--------------------------------------------------------------
# library(asa)
# 
# # Initialize the agent
# agent <- initialize_agent(
#   backend = "openai",
#   model = "gpt-4.1-mini",
#   conda_env = "asa_env"
# )
# 
# # Run a simple query
# result <- run_task(
#   prompt = "What is the current population of Tokyo, Japan?",
#   output_format = "text",
#   agent = agent
# )
# 
# # View the response
# print(result$message)

## ----json-output--------------------------------------------------------------
# result <- run_task(
#   prompt = "Find information about Marie Curie and return JSON with these exact fields:
#             birth_year, death_year, nationality, field_of_study, major_achievement",
#   output_format = "json",
#   agent = agent
# )
# 
# # Access parsed data
# print(result$parsed)
# # $birth_year
# # [1] "1867"
# #
# # $death_year
# # [1] "1934"
# #
# # $nationality
# # [1] "Polish-French"
# # ...

## ----extract-fields-----------------------------------------------------------
# result <- run_task(
#   prompt = "What is the capital of Australia and its population?",
#   output_format = c("capital", "population"),
#   agent = agent
# )
# 
# # Access extracted fields
# result$parsed$capital     # "Canberra"
# result$parsed$population  # "453,000" (approximate)

## ----build-prompt-------------------------------------------------------------
# # Define a reusable template
# template <- "Find the {{metric}} of {{entity}} in {{year}} and return JSON
#              with fields: value, source, confidence"
# 
# # Generate specific prompts
# prompt1 <- build_prompt(template,
#                         metric = "GDP",
#                         entity = "Germany",
#                         year = 2023)
# 
# prompt2 <- build_prompt(template,
#                         metric = "population",
#                         entity = "Brazil",
#                         year = 2024)
# 
# # Run the queries
# result1 <- run_task(prompt1, output_format = "json", agent = agent)
# result2 <- run_task(prompt2, output_format = "json", agent = agent)

## ----batch-simple-------------------------------------------------------------
# # Define your queries
# prompts <- c(
#   "What is the population of Tokyo?",
#   "What is the population of New York City?",
#   "What is the population of London?",
#   "What is the population of Paris?"
# )
# 
# # Run all queries (sequential)
# results <- run_task_batch(prompts, agent = agent, progress = TRUE)
# 
# # Extract the messages
# responses <- sapply(results, function(r) r$message)

## ----batch-dataframe----------------------------------------------------------
# # Create research tasks
# research_df <- data.frame(
#   country = c("France", "Germany", "Japan", "Brazil"),
#   prompt = c(
#     "What is the current head of state of France?",
#     "What is the current head of state of Germany?",
#     "What is the current head of state of Japan?",
#     "What is the current head of state of Brazil?"
#   )
# )
# 
# # Run batch and get results merged back
# results_df <- run_task_batch(
#   research_df,
#   output_format = "text",
#   agent = agent,
#   progress = TRUE
# )
# 
# # Results are added as new columns
# print(results_df[, c("country", "response", "status")])

## ----batch-parallel-----------------------------------------------------------
# # Requires future.apply package
# # install.packages("future.apply")
# 
# results <- run_task_batch(
#   prompts,
#   agent = agent,
#   parallel = TRUE,
#   workers = 4
# )

## ----backend-options----------------------------------------------------------
# # OpenAI - Best for complex reasoning tasks
# agent_openai <- initialize_agent(
#   backend = "openai",
#   model = "gpt-4.1-mini"  # or "gpt-4.1", "gpt-4o"
# )
# 
# # Groq - Fast inference, good for simple queries
# agent_groq <- initialize_agent(
#   backend = "groq",
#   model = "llama-3.3-70b-versatile"
# )
# 
# # xAI - Alternative provider
# agent_xai <- initialize_agent(
#   backend = "xai",
#   model = "grok-beta"
# )

## ----memory-folding-----------------------------------------------------------
# # Enable memory folding (default)
# agent <- initialize_agent(
#   backend = "openai",
#   model = "gpt-4.1-mini",
#   use_memory_folding = TRUE,
#   memory_threshold = 4,      # Fold after 4 messages
#   memory_keep_recent = 2     # Keep 2 most recent messages intact
# )
# 
# # Disable for short, independent queries
# agent_no_fold <- initialize_agent(
#   backend = "openai",
#   model = "gpt-4.1-mini",
#   use_memory_folding = FALSE
# )

## ----rate-limit---------------------------------------------------------------
# agent <- initialize_agent(
#   backend = "openai",
#   model = "gpt-4.1-mini",
#   rate_limit = 0.2,  # 0.2 requests per second (1 every 5 seconds)
#   timeout = 120      # 2 minute timeout per request
# )

## ----tor-setup----------------------------------------------------------------
# # Check if Tor is running
# if (is_tor_running()) {
#   message("Tor is running!")
# 
#   # Initialize agent with Tor proxy
#   agent <- initialize_agent(
#     backend = "openai",
#     model = "gpt-4.1-mini",
#     proxy = "socks5h://127.0.0.1:9050"
#   )
# 
#   # Verify your Tor IP
#   ip <- get_tor_ip()
#   message("Current Tor exit IP: ", ip)
# } else {
#   message("Tor is not running. Start with: brew services start tor")
# }

## ----tor-rotate---------------------------------------------------------------
# # Rotate to a new Tor circuit (new exit node)
# rotate_tor_circuit(method = "brew")  # macOS
# # rotate_tor_circuit(method = "systemctl")  # Linux
# 
# # Verify the new IP
# new_ip <- get_tor_ip()
# message("New Tor exit IP: ", new_ip)

## ----trace-data---------------------------------------------------------------
# # Run a query
# result <- run_task(
#   prompt = "Who won the 2024 Nobel Prize in Physics?",
#   output_format = "text",
#   agent = agent
# )
# 
# # The trace shows all agent steps
# cat(substr(result$raw_output, 1, 500))  # First 500 characters

## ----extract-search-----------------------------------------------------------
# response <- run_agent(
#   prompt = "What are the main causes of climate change?",
#   agent = agent
# )
# 
# # Extract structured data from the trace
# extracted <- extract_agent_results(response$trace)
# 
# # Search snippets from DuckDuckGo
# print(extracted$search_snippets)
# 
# # URLs of sources used
# print(extracted$search_urls)
# 
# # Wikipedia content retrieved
# print(extracted$wikipedia_snippets)

## ----process-outputs----------------------------------------------------------
# # After running a batch
# results_df <- run_task_batch(prompts, agent = agent)
# 
# # Add raw output column for processing
# results_df$raw_output <- sapply(results, function(r) r$raw_output)
# 
# # Process all outputs to extract metadata
# processed_df <- process_outputs(results_df)
# 
# # Now contains search_count, wiki_count, and any JSON fields
# print(processed_df[, c("prompt", "search_count", "wiki_count")])

## ----clear-prompts------------------------------------------------------------
# # Good: Specific and structured
# good_prompt <- "Find the GDP of South Korea in 2023. Return JSON with fields:
#                 gdp_value (in USD), gdp_growth_rate (percentage), data_source"
# 
# # Bad: Vague
# bad_prompt <- "Tell me about South Korea's economy"

## ----error-handling-----------------------------------------------------------
# result <- run_task(prompt, agent = agent)
# 
# if (result$status == "success") {
#   # Process result
#   print(result$message)
# } else {
#   # Handle error
#   warning("Query failed: ", result$raw_output)
# }

## ----check-backend------------------------------------------------------------
# # Verify environment before running queries
# status <- check_backend(conda_env = "asa_env")
# 
# if (!status$available) {
#   message("Missing packages: ", paste(status$missing_packages, collapse = ", "))
#   build_backend()  # Rebuild if needed
# }

## ----agent-state--------------------------------------------------------------
# # Get current agent (if initialized)
# current_agent <- get_agent()
# 
# # Reset agent state between different tasks
# reset_agent()
# 
# # Re-initialize with new settings
# agent <- initialize_agent(backend = "groq", model = "llama-3.3-70b-versatile")

## ----complete-example---------------------------------------------------------
# library(asa)
# library(dplyr)
# 
# # 1. Setup
# build_backend(conda_env = "asa_env")
# 
# agent <- initialize_agent(
#   backend = "openai",
#   model = "gpt-4.1-mini",
#   conda_env = "asa_env",
#   use_memory_folding = TRUE
# )
# 
# # 2. Define research questions
# countries <- c("Norway", "Sweden", "Denmark", "Finland", "Iceland")
# 
# research_df <- data.frame(
#   country = countries,
#   prompt = sapply(countries, function(c) {
#     build_prompt(
#       "Research {{country}} and return JSON with:
#        population (number), capital (city name),
#        gdp_per_capita (USD), happiness_rank (global ranking)",
#       country = c
#     )
#   })
# )
# 
# # 3. Run batch research
# results_df <- run_task_batch(
#   research_df,
#   output_format = "json",
#   agent = agent,
#   progress = TRUE
# )
# 
# # 4. Analyze results
# summary_df <- results_df %>%
#   select(country, population, capital, gdp_per_capita, happiness_rank, status) %>%
#   filter(status == "success")
# 
# print(summary_df)
# 
# # 5. Save results
# write.csv(summary_df, "nordic_countries_research.csv", row.names = FALSE)

## ----troubleshoot-1-----------------------------------------------------------
# # Always initialize before running tasks
# agent <- initialize_agent(backend = "openai", model = "gpt-4.1-mini")

## ----troubleshoot-2-----------------------------------------------------------
# # Rebuild the Python environment
# build_backend(conda_env = "asa_env")

## ----troubleshoot-3-----------------------------------------------------------
# # Reduce request rate
# agent <- initialize_agent(
#   backend = "openai",
#   model = "gpt-4.1-mini",
#   rate_limit = 0.1  # 1 request per 10 seconds
# )

## ----troubleshoot-4-----------------------------------------------------------
# # Check the trace for error details
# result <- run_task(prompt, agent = agent)
# if (is.na(result$message)) {
#   cat("Error trace:\n", result$raw_output)
# }

