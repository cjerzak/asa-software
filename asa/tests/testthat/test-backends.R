# test-backends.R - Multi-provider integration tests
# Uses loop-based approach to test all supported backends

# Backend configurations: (backend, model, env_var)
# Updated Dec 2025 with current model IDs
backends <- list(
  list(backend = "openai", model = "gpt-4o-mini", env = "OPENAI_API_KEY"),
  list(backend = "groq", model = "llama-3.3-70b-versatile", env = "GROQ_API_KEY"),
  list(backend = "xai", model = "grok-2-1212", env = "XAI_API_KEY"),
  list(
    backend = "gemini",
    model = "gemini-3-flash-preview",
    env = c("GOOGLE_API_KEY", "GEMINI_API_KEY"),
    py_module = "langchain_google_genai"
  ),
  list(backend = "openrouter", model = "google/gemini-2.0-flash-exp:free", env = "OPENROUTER_API_KEY"),
  list(backend = "openrouter", model = "meta-llama/llama-3.3-70b-instruct:free", env = "OPENROUTER_API_KEY"),
  list(backend = "openrouter", model = "deepseek/deepseek-r1:free", env = "OPENROUTER_API_KEY")
)

skip_api_tests <- tolower(Sys.getenv("ASA_CI_SKIP_API_TESTS")) %in% c("true", "1", "yes")

# Test agent initialization for each backend
for (cfg in backends) {
  test_that(paste0(cfg$backend, "/", cfg$model, " - agent initialization"), {
    skip_if(skip_api_tests, "ASA_CI_SKIP_API_TESTS is set")
    if (!is.null(cfg$py_module)) {
      skip_if(
        !reticulate::py_module_available(cfg$py_module),
        paste("Missing Python module", cfg$py_module)
      )
    }
    skip_if(
      !any(nzchar(Sys.getenv(cfg$env))),
      paste("Missing", paste(cfg$env, collapse = " or "))
    )

    agent <- initialize_agent(
      backend = cfg$backend,
      model = cfg$model,
      verbose = FALSE
    )
    expect_s3_class(agent, "asa_agent")
    expect_equal(agent$backend, cfg$backend)
  })
}

# Test simple query for each backend
for (cfg in backends) {
  test_that(paste0(cfg$backend, "/", cfg$model, " - simple query"), {
    skip_if(skip_api_tests, "ASA_CI_SKIP_API_TESTS is set")
    if (!is.null(cfg$py_module)) {
      skip_if(
        !reticulate::py_module_available(cfg$py_module),
        paste("Missing Python module", cfg$py_module)
      )
    }
    skip_if(
      !any(nzchar(Sys.getenv(cfg$env))),
      paste("Missing", paste(cfg$env, collapse = " or "))
    )

    agent <- initialize_agent(
      backend = cfg$backend,
      model = cfg$model,
      verbose = FALSE
    )
    result <- run_task(
      prompt = "What is 2+2? Reply with just the number.",
      output_format = "text",
      agent = agent
    )
    # Collapse result to single string and check for standalone "4"
    result_text <- paste(unlist(result), collapse = " ")
    expect_true(grepl("\\b4\\b", result_text))
  })
}
