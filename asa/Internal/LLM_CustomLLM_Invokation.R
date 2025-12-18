# Script: LLM_CustomLLM.R
#
# DeepAgent-style Memory Folding Configuration:
#   Set USE_MEMORY_FOLDING <- TRUE to enable autonomous memory folding
#   This implements the architecture from the DeepAgent paper where
#   the agent automatically summarizes older messages to manage context.
#
#   Configuration options (set before sourcing this file):
#     USE_MEMORY_FOLDING     - Enable memory folding (default: TRUE)
#     MEMORY_THRESHOLD       - Messages before folding triggers (default: 6)
#     MEMORY_KEEP_RECENT     - Messages to keep after folding (default: 2)
#     MEMORY_DEBUG           - Enable debug logging (default: FALSE)
#
{
# Set defaults for memory folding if not already defined
if (!exists("USE_MEMORY_FOLDING")) USE_MEMORY_FOLDING <- TRUE
if (!exists("MEMORY_THRESHOLD")) MEMORY_THRESHOLD <- 4L
if (!exists("MEMORY_KEEP_RECENT")) MEMORY_KEEP_RECENT <- 2L
if (!exists("MEMORY_DEBUG")) MEMORY_DEBUG <- FALSE
if (!exists("AGENT_MAX_STEPS")) AGENT_MAX_STEPS <- 3L # Default steps before forced stop

# Run the agent, if initialized
if(INITIALIZED_CUSTOM_ENV_TAG){
    # rebuild agent - make sure tor is running (see tor browser to confirm)
    # brew services start tor
    # rotating is potentially dangerous:
    # https://chatgpt.com/share/68577668-1518-800f-b27d-4ae400938b17

    # define proxy
    {
      #pip install httpx[http2]
      theProxy <- Sys.getenv("HTTP_PROXY")
      theProxy <- ifelse(Sys.getenv("HTTP_PROXY") == "",
                         yes = "socks5h://127.0.0.1:9050",
                         no = Sys.getenv("HTTP_PROXY"))
      # Debug output removed for security - uncomment for debugging only:
      # write.csv(file = "~/Downloads/proxy.csv",theProxy)
      # write.csv(file = "~/Downloads/env.csv",Sys.getenv())
      
      #theHeader <- header_options[[sample(length(header_options),1)]]
      theHeader <- fake_headers$Headers(headers = TRUE)$generate()
      
      sync_client <- httpx$Client(
        #trust_env     = TRUE,  # don't both set trust_env to TRUE and add a proxy
        proxy          =  theProxy,  # or omit if you rely on trust_env
        timeout        = 120,                    # Timeout in seconds
        http2          = FALSE,
        headers = theHeader,
        follow_redirects  = TRUE
      )
      async_client <- httpx$AsyncClient(
        #trust_env     = TRUE,  # will be on by default 
        proxy          = theProxy,
        timeout        = 120,
        http2          = FALSE,
        headers = theHeader,
        follow_redirects  = TRUE
      )
    }
    
    # define tools 
    {
    # Then in execution: rotate_tor_circuit()
    wiki <- community_tools$WikipediaQueryRun(
      api_wrapper = community_utils$WikipediaAPIWrapper(
      top_k_results = 5L,
      doc_content_chars_max = 1000),
      verbose = FALSE,
      timeout = 90L,
      proxy = theProxy,
      max_concurrency = 1L
    )
      
    # package ddg - does not seem to use proxies or headers well 
    if(FALSE){ 
      search <- community_tools$DuckDuckGoSearchRun(name = "Search",
                                                      api_wrapper = community_utils$DuckDuckGoSearchAPIWrapper(
                                                        max_results = 1L,
                                                        safesearch = "moderate",
                                                        time = "none"
                                                      ),
                                                      proxy = theProxy,
                                                      doc_content_chars_max = 500L, # likely ignored 
                                                      timeout = 90L,
                                                      verbose = FALSE,
                                                      max_concurrency = 1L)  
    }
    

    if(TRUE){
    custom_ddg <- reticulate::import_from_path("custom_ddg_production", path = "~/Dropbox/APIs/AgentFunctions/")
    #custom_ddg <- reticulate::import_from_path("custom_ddg_experimental", path = "~/Dropbox/APIs/AgentFunctions/")
    search <- custom_ddg$PatchedDuckDuckGoSearchRun(
      name        = "Search",
      description = "DuckDuckGo web search",
      api_wrapper = custom_ddg$PatchedDuckDuckGoSearchAPIWrapper(
        proxy       = theProxy,           # socks5h://127.0.0.1:9050
        #headers     = theHeader, 
        use_browser = TRUE,
        #verify = FALSE, 
        max_results = 10L,
        safesearch  = "moderate",
        time        = "none"
      ),
      doc_content_chars_max = 500L,
      timeout     = 90L,
      verbose     = FALSE,
      max_concurrency = 1L
    )

    # sanity checks 
    # search$run("who is john galt")
    # tmp <- system.time( tmp_ <- search$run("who is john galt")  ) ; tmp
    # search$invoke("who is john galt") 
    # returned_content <- search$invoke(sprintf("capital of %s",sample(state.abb,1)))
    }
      
    # save tools 
    theTools <- list(wiki, search)
    }
    
    # recreate LLM + agent 
    if(CustomLLMBackend == "groq"){
      theLLM <- chat_models$ChatGroq(
        groq_api_key = Sys.getenv("GROQ_API_KEY"),
        model = modelName,
        temperature = 0.01,
        streaming = TRUE,
        rate_limiter = RateLimiter
      )
    }
    if(CustomLLMBackend == "exo"){
      base_url <- system("ifconfig | grep 'inet ' | awk '{print $2}'", intern = TRUE)
      base_url <- sprintf("%s:52415", base_url[length(base_url)])
      
      exo_url <- sprintf("http://%s/v1",base_url)
      
      # import the ChatOpenAI class
      # https://python.langchain.com/docs/integrations/chat/
      # https://chatgpt.com/share/684ce13e-20b8-800f-b7a6-d3909cd9da02
      chat_models <- import("langchain_openai")
      source('./Analysis/LLM_ExoLLMWrapper.R', local = TRUE)
      
      # instantiate it against your Exo endpoint
      #theLLM <- chat_models$ChatOpenAI(model_name = modelName, openai_api_base = exo_url,temperature = 0.01,streaming = TRUE)
      
      # little sanity test for the exo model 
      if(FALSE){
        # extract the first generation’s text
        schema  <- import("langchain.schema",  convert = FALSE)
        sys_msg <- schema$SystemMessage(content = "You are a helpful assistant.")
        usr_msg <- schema$HumanMessage(content = "What model are you?") 
        theLLM$invoke(input = "What is the capital of Egypt?")
        (theLLM$bind_tools(theTools))$invoke(input = "What is the capital of Egypt?")
        theLLM$generate( list( list(sys_msg, usr_msg) ) )
      }
    }
    if(CustomLLMBackend == "openai"){
      # import the OpenAI chat wrapper
      theLLM <- chat_models$ChatOpenAI(
        # model management 
        model_name      = modelName,
        openai_api_key  = Sys.getenv("OPENAI_API_KEY"),
        openai_api_base = Sys.getenv("OPENAI_API_BASE", unset = "https://api.openai.com/v1"),
        temperature     = 0.5,
        streaming = TRUE,
        
        # tor management 
        #default_headers = header_options[[sample(length(header_options),1)]], # belong in client 
        http_client  = sync_client,
        http_async_client = async_client,
        include_response_headers = FALSE, 
        rate_limiter = RateLimiter
      )
    }
  
    #tor_ip <- try(system(sprintf(
      #"curl -s --proxy %s https://api.ipify.org?format=json",theProxy),intern = TRUE),T); message("IP seen: ", tor_ip)

    #cmd <- sprintf("curl -s --proxy %s https://check.torproject.org/api/ip", theProxy)
    #out <- try(system(cmd, intern=TRUE), silent=TRUE)
    #message("Tor check result: ", out)

    # define agent (important to cleanse memory)
    # Choose between standard prebuilt agent or DeepAgent-style memory folding
    if (USE_MEMORY_FOLDING) {
      message(sprintf("Creating Memory Folding Agent (threshold=%d, keep_recent=%d)",
                      MEMORY_THRESHOLD, MEMORY_KEEP_RECENT))
      theAgent <- custom_ddg$create_memory_folding_agent_with_checkpointer(
        model        = theLLM,
        tools        = theTools,
        checkpointer = MemorySaver(),
        message_threshold = as.integer(MEMORY_THRESHOLD),
        keep_recent  = as.integer(MEMORY_KEEP_RECENT),
        debug        = MEMORY_DEBUG
      )
    } else {
      message("Creating standard prebuilt ReAct agent")
      theAgent <- langgraph$prebuilt$create_react_agent(
        model        = theLLM,
        tools        = theTools,
        checkpointer = MemorySaver(),
        debug = FALSE
      )
    }

    # thePrompt <- "What is the political party of Scott Walker?"
    # search$invoke("Capital of Egypt")
    # (theLLM$bind_tools(theTools))$invoke(input = thePrompt)
    t0 <- Sys.time()

    # Build initial state based on agent type
    # Memory folding agent uses a different state schema with summary field
    if (USE_MEMORY_FOLDING) {
      # Import HumanMessage for proper message typing
      from_schema <- import("langchain_core.messages")
      initial_message <- from_schema$HumanMessage(content = thePrompt)

      initial_state <- list(
        messages = list(initial_message),
        summary = "",
        fold_count = 0L
      )
      raw_response <- try(theAgent$invoke(
        initial_state,
        config = list(
          configurable = list(thread_id = rlang::hash(Sys.time())),
          recursion_limit = 100L  # Higher limit for memory folding loops
        )
      ), TRUE)
    } else {
      raw_response <- try(theAgent$invoke(
        list(messages = list(list(role = "user", content = thePrompt))),
        config = list(
          configurable = list(thread_id = rlang::hash(Sys.time())),
          recursion_limit = 20L
        )
      ), TRUE)
    }
    print(sprintf("Agent ran in %.3f mins",
                  difftime(Sys.time(),t0,units = "mins")))
    print(raw_response)

    # Report memory folding statistics if applicable
    if (USE_MEMORY_FOLDING && !inherits(raw_response, "try-error")) {
      fold_count <- tryCatch(raw_response$fold_count, error = function(e) 0)
      summary_len <- tryCatch(nchar(raw_response$summary %||% ""), error = function(e) 0)
      msg_count <- tryCatch(length(raw_response$messages), error = function(e) 0)
      message(sprintf("Memory Folding Stats: folds=%d, summary_chars=%d, final_messages=%d",
                      fold_count %||% 0, summary_len, msg_count))
    }

    # save full traceback
    text_blob <- paste(lapply(unlist(raw_response),function(l_){capture.output(l_)}), collapse = "\n\n")
    #writeBin(charToRaw(text_blob), file.path(sprintf("%s/traces/i%i.txt", output_directory,i)))
    
    if(grepl(text_blob,pattern = "Ratelimit")){
      #rotate_tor_circuit()  
      Sys.sleep(15L); warning("202 Ratelimit triggered")
    } # triggered on task 44 
    if(grepl(text_blob,pattern = "Timeout")){
      #rotate_tor_circuit()  
      Sys.sleep(15L); warning("Timeout triggered")
    }
    
    # extract final response 
    theResponseText <- try(raw_response$messages[[length(raw_response$messages)]]$text(), TRUE)
    
    # clean text
    theResponseText <- gsub("<[^>]+>.*?</[^>]+>\\n?", "", theResponseText)
    if(CustomLLMBackend == "exo"){
      theResponseText <- sub(
        "(?s).*<\\|python_tag\\|>(\\{.*?\\})<\\|eom_id\\|>.*",
        "\\1", theResponseText, perl = TRUE)
    }

    if(class(theResponseText) %in% "try-error"){
      response <- try(list("message" = NA, "status_code" = 100), TRUE)
    }
    if(!class(theResponseText) %in% "try-error"){
      response <- try(list("message" = theResponseText, "status_code" = 200), TRUE)
    }
}
  
# Initialize the agent 
if(!INITIALIZED_CUSTOM_ENV_TAG){
      INITIALIZED_CUSTOM_ENV_TAG <- TRUE
      # define toolkit using tor 
      # brew services start tor 
      # from terminal
      #system("brew services start tor")
      #Sys.setenv(all_proxy   = "socks5h://127.0.0.1:9050",
                 #http_proxy  = "socks5h://127.0.0.1:9050",
                 #https_proxy = "socks5h://127.0.0.1:9050")
  
  
      # conda create -n CustomLLMSearch python=3.13
      # conda activate CustomLLMSearch
      # pip install uv 
      # uv pip install --upgrade uv streamlit fake_headers langchain_groq langchain_community langgraph python-dotenv langchain_openai arxiv wikipedia ddgs socksio pysocks requests langchain-tavily beautifulsoup4 selenium primp

      # activate env 
      library(reticulate)
      use_condaenv("CustomLLMSearch", required = TRUE)
      
      # load packages 
      community_utils <- import("langchain_community.utilities")
      community_tools <- import("langchain_community.tools")
      langgraph <- import("langgraph")
      MemorySaver <- import("langgraph.checkpoint.memory")$MemorySaver
      # langchain_tavily <- import("langchain_tavily")
      # dotenv <- import("dotenv"); dotenv$load_dotenv()
      
      
      #rotate_tor_circuit <- function() { # not reliable?
      #  system("kill -HUP $(pgrep -x tor)")  # Send SIGHUP to Tor process
      #  Sys.sleep(5)  # Allow circuit rebuild
      #}
      rotate_tor_circuit <- function() {
        system("brew services restart tor")  
        Sys.sleep(12)
      }

    # define rate limiter 
    RateLimit <- import("langchain_core.rate_limiters")
    RateLimiter = RateLimit$InMemoryRateLimiter(
      requests_per_second=0.2,  # <-- We can make a request once every 1/value seconds;  30/min limit?
      check_every_n_seconds=0.1,  # Wake up every value ms to check whether allowed to make a request,
      max_bucket_size=1,  # Controls the maximum burst size.
    )
    httpx <- import("httpx")
    fake_headers<- import("fake_headers", convert = FALSE)

    if(CustomLLMBackend %in% c("exo","openai")){
      chat_models <- import("langchain_openai")
    }
    if(CustomLLMBackend %in% c("groq")){ 
      chat_models <- import("langchain_groq")
    }
    if(CustomLLMBackend %in% c("xai")){ 
      chat_models <- import("langchain_groq")
    }
    
    # sanity checks
    if(FALSE){
      search$description
      
      from_schema <- import("langchain_core.messages")
      thePrompt <- "What is the political party of Scott Walker?"
      thePrompt2 <- list(from_schema$HumanMessage(content = thePrompt))
   
      # probe base output with tools       
      (theLLM$bind_tools(theTools))$invoke(input = thePrompt)
      (theLLM$bind_tools(theTools))$invoke(input = thePrompt2)

      # probe interactive agent use 
      theAgent <- langgraph$prebuilt$create_react_agent(model = theLLM, 
                                                        tools = theTools,
                                                        checkpointer = MemorySaver())
      
      theAgent$invoke(
        list(messages = list(list(role = "user", 
                                  content = thePrompt))),
        config = list(configurable = list(thread_id = "session-001"))
      )
      theAgent$invoke(
        list(messages = thePrompt2),
        config = list(configurable = list(thread_id = "session-001"))
      )
      if("search" %in% ls()){ 
      tryCatch({
        result <- search$invoke("scott walker age")
        print("Search working!")
        print(result)
      }, error = function(e) {
        print(paste("Search tool failing:", e$message))
      })
    }
    }
    
    message("Done setting up Custom LLM...")
}
}
