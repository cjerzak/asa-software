# test-backends.R - Multi-provider integration tests
# Uses loop-based approach to test all supported backends

# Backend configurations: (backend, model, env_var)
# Updated Mar 2026 with stable smoke-test model IDs
backends <- list(
  list(backend = "openai", model = "gpt-4o-mini", env = "OPENAI_API_KEY"),
  list(backend = "groq", model = "llama-3.3-70b-versatile", env = "GROQ_API_KEY"),
  list(backend = "xai", model = "grok-3-mini", env = "XAI_API_KEY"),
  list(
    backend = "gemini",
    model = "gemini-3-flash-preview",
    env = c("GOOGLE_API_KEY", "GEMINI_API_KEY"),
    py_module = "langchain_google_genai"
  ),
  list(backend = "openrouter", model = "google/gemini-2.0-flash-001", env = "OPENROUTER_API_KEY"),
  list(backend = "openrouter", model = "openai/gpt-4.1-mini", env = "OPENROUTER_API_KEY")
)

.extract_numeric_answer <- function(text) {
  if (!is.character(text) || length(text) == 0L) {
    return(NA_real_)
  }
  value <- trimws(as.character(text[[1]]))
  if (!nzchar(value)) {
    return(NA_real_)
  }

  if (grepl("^[-+]?[0-9]+(?:\\.[0-9]+)?$", value, perl = TRUE)) {
    return(suppressWarnings(as.numeric(value)))
  }

  nums <- regmatches(
    value,
    gregexpr("(?<![0-9])[-+]?[0-9]+(?:\\.[0-9]+)?(?![0-9])", value, perl = TRUE)
  )[[1]]
  if (length(nums) == 0L) {
    return(NA_real_)
  }

  suppressWarnings(as.numeric(nums[[length(nums)]]))
}

.backend_invoke_error_info <- function(result) {
  invoke_error <- result$execution$invoke_error %||% NULL
  if (!is.list(invoke_error) || length(invoke_error) == 0L) {
    return("")
  }

  error_type <- as.character(invoke_error$error_type %||% "")
  error_message <- as.character(invoke_error$error_message %||% "")
  parts <- c()
  if (nzchar(error_type)) {
    parts <- c(parts, paste("error_type:", error_type))
  }
  if (nzchar(error_message)) {
    parts <- c(parts, paste("error_message:", substr(error_message, 1, 200)))
  }
  if (length(parts) == 0L) {
    return("")
  }

  paste(parts, collapse = " ")
}

.with_backend_agent <- function(cfg, expr) {
  asa_test_skip_api_tests()
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
    search = search_options(stability_profile = "deterministic"),
    verbose = FALSE
  )
  eval(substitute(expr), envir = list2env(list(agent = agent, cfg = cfg), parent = parent.frame()))
}

# Test agent initialization for each backend
for (cfg in backends) {
  test_that(paste0(cfg$backend, "/", cfg$model, " - agent initialization"), {
    .with_backend_agent(cfg, {
      expect_s3_class(agent, "asa_agent")
      expect_equal(agent$backend, cfg$backend)
    })
  })
}

# Test simple query for each backend
for (cfg in backends) {
  test_that(paste0(cfg$backend, "/", cfg$model, " - simple query"), {
    .with_backend_agent(cfg, {
      result <- run_task(
        prompt = "What is 2+2? Reply with just the number.",
        output_format = "text",
        agent = agent
      )
      error_info <- .backend_invoke_error_info(result)
      result_text <- if (is.null(result$message) || length(result$message) == 0 || is.na(result$message[1])) {
        ""
      } else {
        as.character(result$message[1])
      }
      expect_identical(
        result$status,
        "success",
        info = paste(
          "Expected success status; got:", result$status,
          "message:", substr(result_text, 1, 200),
          error_info
        )
      )
      answer_num <- .extract_numeric_answer(result_text)
      expect_true(
        is.finite(answer_num) && isTRUE(all.equal(answer_num, 4, tolerance = 1e-9)),
        info = paste("Expected numeric answer 4; got:", substr(result_text, 1, 200), error_info)
      )
    })
  })
}
