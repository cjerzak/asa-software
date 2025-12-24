# Tests for agent state management (get_agent, reset_agent)

test_that("get_agent returns NULL when not initialized", {
  # Reset any existing agent first
  reset_agent()

  agent <- get_agent()
  expect_null(agent)
})

test_that("reset_agent clears agent state", {
  skip_on_ci()
  skip("Requires conda environment and API keys")

  # Initialize an agent
  agent <- initialize_agent(
    backend = "openai",
    model = "gpt-4.1-mini",
    verbose = FALSE
  )

  expect_s3_class(agent, "asa_agent")

  # Reset should clear it
  reset_agent()

  # get_agent should now return NULL
  expect_null(get_agent())
})

test_that("reset_agent is idempotent", {
  # Calling reset multiple times should not error
  expect_error(reset_agent(), NA)
  expect_error(reset_agent(), NA)
  expect_error(reset_agent(), NA)
})

test_that("get_agent returns correct agent type", {
  skip_on_ci()
  skip("Requires conda environment and API keys")

  agent <- initialize_agent(
    backend = "openai",
    model = "gpt-4.1-mini",
    verbose = FALSE
  )

  retrieved_agent <- get_agent()

  expect_s3_class(retrieved_agent, "asa_agent")
  expect_equal(retrieved_agent$backend, "openai")
  expect_equal(retrieved_agent$model, "gpt-4.1-mini")

  reset_agent()
})

test_that("get_agent preserves agent configuration", {
  skip_on_ci()
  skip("Requires conda environment and API keys")

  agent <- initialize_agent(
    backend = "groq",
    model = "llama-3.3-70b-versatile",
    use_memory_folding = FALSE,
    verbose = FALSE
  )

  retrieved_agent <- get_agent()

  expect_equal(retrieved_agent$config$backend, "groq")
  expect_equal(retrieved_agent$config$model, "llama-3.3-70b-versatile")
  expect_false(retrieved_agent$config$use_memory_folding)

  reset_agent()
})

test_that("agent persists across multiple get_agent calls", {
  skip_on_ci()
  skip("Requires conda environment and API keys")

  agent1 <- initialize_agent(backend = "openai", model = "gpt-4.1-mini", verbose = FALSE)

  # Multiple gets should return the same agent
  agent2 <- get_agent()
  agent3 <- get_agent()

  expect_equal(agent2$backend, agent1$backend)
  expect_equal(agent3$backend, agent1$backend)
  expect_equal(agent2$config$backend, agent1$config$backend)

  reset_agent()
})

test_that("reset_agent allows reinitialization", {
  skip_on_ci()
  skip("Requires conda environment and API keys")

  # Initialize with one backend
  agent1 <- initialize_agent(backend = "openai", model = "gpt-4.1-mini", verbose = FALSE)
  expect_equal(agent1$backend, "openai")

  # Reset
  reset_agent()

  # Initialize with different backend
  agent2 <- initialize_agent(backend = "groq", model = "llama-3.3-70b-versatile", verbose = FALSE)
  expect_equal(agent2$backend, "groq")

  # Verify it's a new agent
  retrieved <- get_agent()
  expect_equal(retrieved$backend, "groq")

  reset_agent()
})

test_that("agent created_at timestamp is set", {
  skip_on_ci()
  skip("Requires conda environment and API keys")

  before <- Sys.time()
  agent <- initialize_agent(backend = "openai", model = "gpt-4.1-mini", verbose = FALSE)
  after <- Sys.time()

  expect_true(!is.null(agent$created_at))
  expect_s3_class(agent$created_at, "POSIXct")
  expect_true(agent$created_at >= before)
  expect_true(agent$created_at <= after)

  reset_agent()
})

test_that("run_agent with NULL agent uses get_agent", {
  skip_on_ci()
  skip("Requires conda environment and API keys")

  agent <- initialize_agent(backend = "openai", model = "gpt-4.1-mini", verbose = FALSE)

  # run_agent with agent = NULL should use the initialized agent
  # This would fail if agent not initialized
  expect_error(
    run_agent("Test prompt", agent = NULL, verbose = FALSE),
    NA
  )

  reset_agent()
})

test_that("run_agent errors when agent not initialized and NULL passed", {
  reset_agent()

  expect_error(
    run_agent("Test prompt", agent = NULL),
    "not initialized"
  )
})

test_that("agent memory folding state persists", {
  skip_on_ci()
  skip("Requires conda environment and API keys")

  # Initialize with memory folding enabled
  agent1 <- initialize_agent(
    backend = "openai",
    model = "gpt-4.1-mini",
    use_memory_folding = TRUE,
    memory_threshold = 10L,
    memory_keep_recent = 5L,
    verbose = FALSE
  )

  retrieved <- get_agent()

  expect_true(retrieved$config$use_memory_folding)
  expect_equal(retrieved$config$memory_threshold, 10L)
  expect_equal(retrieved$config$memory_keep_recent, 5L)

  reset_agent()
})

test_that("agent proxy configuration persists", {
  skip_on_ci()
  skip("Requires conda environment and API keys")

  agent <- initialize_agent(
    backend = "openai",
    model = "gpt-4.1-mini",
    proxy = "socks5h://127.0.0.1:9050",
    verbose = FALSE
  )

  retrieved <- get_agent()
  expect_equal(retrieved$config$proxy, "socks5h://127.0.0.1:9050")

  reset_agent()
})

test_that("reset_agent handles uninitialized state gracefully", {
  # Should not error even if nothing to reset
  reset_agent()
  expect_error(reset_agent(), NA)

  # get_agent should still return NULL
  expect_null(get_agent())
})

test_that("multiple initializations without reset override", {
  skip_on_ci()
  skip("Requires conda environment and API keys")

  agent1 <- initialize_agent(backend = "openai", model = "gpt-4.1-mini", verbose = FALSE)
  agent2 <- initialize_agent(backend = "groq", model = "llama-3.3-70b-versatile", verbose = FALSE)

  # Second initialization should override
  retrieved <- get_agent()
  expect_equal(retrieved$backend, "groq")

  reset_agent()
})

# Test internal state management
test_that("package environment is properly managed", {
  # After reset, package environment should be cleared
  reset_agent()

  # These are internal checks - the environment should exist
  # but should be in uninitialized state
  expect_true(exists("asa_env"))
})

test_that("reset_agent returns NULL invisibly", {
  result <- reset_agent()
  expect_null(result)
})

test_that("get_agent with reinitialization works", {
  skip_on_ci()
  skip("Requires conda environment and API keys")

  # Reset first
  reset_agent()
  expect_null(get_agent())

  # Initialize
  agent <- initialize_agent(backend = "openai", model = "gpt-4.1-mini", verbose = FALSE)
  expect_s3_class(get_agent(), "asa_agent")

  reset_agent()
})
