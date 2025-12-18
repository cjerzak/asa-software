# Script: LLM_ProcessPredictionOutputs.R
# Processes predictions for replication (assumes input name of df)
# Note: Assumes access to dplyr for  %>% 
{
  # Helper functions 
  {
    # agent results - string process fxns
    {
      library(jsonlite)
      clean_traces <- function(text) {
        # Initialize return values
        cleaned_traces <- c()
        justification <- NA_character_
        predicted_value <- NA_integer_
        confidence <- NA_character_
        
        tryCatch({
          # 1. Extract all ToolMessage contents more robustly
          # This pattern handles both single and double quotes and captures content more reliably
          tool_pattern <- "ToolMessage\\(content=(?:'([^']*(?:\\\\'[^']*)*)'|\"([^\"]*(?:\\\\\"[^\"]*)*)\"), name=(?:'([^']+)'|\"([^\"]+)\")"
          
          # Find all tool message matches
          tool_matches <- gregexpr(tool_pattern, text, perl = TRUE)[[1]]
          
          if (tool_matches[1] != -1) {
            # Extract matches
            match_texts <- regmatches(text, list(tool_matches))[[1]]
            
            for (match_text in match_texts) {
              # Extract content and tool name
              content_match <- regmatches(match_text, regexec(tool_pattern, match_text, perl = TRUE))[[1]]
              
              # Content could be in position 2 or 3 depending on quote type
              content <- ifelse(nchar(content_match[2]) > 0, content_match[2], content_match[3])
              # Tool name could be in position 4 or 5
              tool_name <- ifelse(nchar(content_match[4]) > 0, content_match[4], content_match[5])
              
              # Clean escape sequences in content
              if (!is.na(content) && nchar(content) > 0) {
                content <- gsub("\\\\n", " ", content)
                content <- gsub("\\\\'", "'", content)
                content <- gsub('\\\\"', '"', content)
                content <- gsub("\\\\\\\\", "\\\\", content)
                
                # Process based on tool type
                if (tool_name %in% c("Search", "search")) {
                  # Extract search results
                  if (grepl("__START_OF_SOURCE", content) || grepl("<CONTENT>", content)) {
                    # Pattern for extracting content between tags
                    source_pattern <- "<CONTENT>(.*?)</CONTENT>"
                    source_matches <- gregexpr(source_pattern, content, perl = TRUE)[[1]]
                    
                    if (source_matches[1] != -1) {
                      sources <- regmatches(content, list(source_matches))[[1]]
                      for (source in sources) {
                        cleaned_source <- gsub(source_pattern, "\\1", source, perl = TRUE)
                        cleaned_source <- trimws(gsub("\\s+", " ", cleaned_source))
                        if (nchar(cleaned_source) > 0) {
                          cleaned_traces <- c(cleaned_traces, cleaned_source)
                        }
                      }
                    }
                  }
                } else if (tool_name %in% c("wikipedia", "Wikipedia")) {
                  # Process Wikipedia results
                  if ((grepl("Page:", content) || grepl("Summary:", content)) && 
                      !grepl("No good Wikipedia Search Result", content)) {
                    cleaned <- trimws(gsub("\\s+", " ", content))
                    if (nchar(cleaned) > 0) {
                      cleaned_traces <- c(cleaned_traces, cleaned)
                    }
                  }
                }
              }
            }
          }
          
          # 2. Extract JSON from AIMessage more flexibly
          # Try multiple patterns to find JSON content
          json_patterns <- c(
            # Pattern 1: AIMessage with content containing JSON
            "AIMessage\\(content='(\\{[^']*(?:\\\\'[^']*)*\\})'",
            # Pattern 2: With double quotes
            "AIMessage\\(content=\"(\\{[^\"]*(?:\\\\\"[^\"]*)*\\})\"",
            # Pattern 3: More general pattern that might catch nested braces
            "AIMessage\\(content=['\"]([\\s\\S]*?)['\"], additional_kwargs",
            # Pattern 4: Look for standalone JSON in the text
            "(\\{\\s*[\"']justification[\"']\\s*:.*?\\})"
          )
          
          json_found <- FALSE
          
          for (pattern in json_patterns) {
            if (!json_found) {
              matches <- regmatches(text, gregexpr(pattern, text, perl = TRUE))[[1]]
              
              if (length(matches) > 0) {
                # Process each match, prefer the last one
                for (i in rev(seq_along(matches))) {
                  if (!json_found) {
                    # Extract JSON string
                    json_str <- gsub(pattern, "\\1", matches[i], perl = TRUE)
                    
                    # Only process if it looks like it contains our expected fields
                    if (grepl("justification|predicted_value|confidence", json_str)) {
                      # Clean escape sequences
                      json_str <- gsub("\\\\n", "\n", json_str)
                      json_str <- gsub('\\\\"', '"', json_str)
                      json_str <- gsub("\\\\'", "'", json_str)
                      json_str <- gsub("\\\\\\\\", "\\\\", json_str)
                      
                      # Try to parse JSON
                      tryCatch({
                        data <- fromJSON(json_str)
                        
                        # Extract values with proper null checking
                        if (!is.null(data$justification)) {
                          justification <- as.character(data$justification)
                        }
                        
                        if (!is.null(data$predicted_value)) {
                          # Handle both numeric and character predicted values
                          pv <- data$predicted_value
                          if (is.character(pv)) {
                            pv <- as.integer(gsub("[^0-9]", "", pv))
                          }
                          predicted_value <- as.integer(pv)
                        }
                        
                        if (!is.null(data$confidence)) {
                          confidence <- as.character(data$confidence)
                        }
                        
                        json_found <- TRUE
                      }, error = function(e) {
                        # Continue to next match/pattern
                      })
                    }
                  }
                }
              }
            }
          }
          
          # 3. If no JSON found in AIMessage, try to find it in the raw text
          if (!json_found) {
            # Look for JSON-like structures in the entire text
            json_like_pattern <- "\\{[^{}]*\"justification\"[^{}]*\"predicted_value\"[^{}]*\"confidence\"[^{}]*\\}"
            json_matches <- regmatches(text, gregexpr(json_like_pattern, text, perl = TRUE))[[1]]
            
            if (length(json_matches) > 0) {
              # Try the last match
              for (i in rev(seq_along(json_matches))) {
                if (!json_found) {
                  tryCatch({
                    data <- fromJSON(json_matches[i])
                    
                    if (!is.null(data$justification)) {
                      justification <- as.character(data$justification)
                    }
                    
                    if (!is.null(data$predicted_value)) {
                      pv <- data$predicted_value
                      if (is.character(pv)) {
                        pv <- as.integer(gsub("[^0-9]", "", pv))
                      }
                      predicted_value <- as.integer(pv)
                    }
                    
                    if (!is.null(data$confidence)) {
                      confidence <- as.character(data$confidence)
                    }
                    
                    json_found <- TRUE
                  }, error = function(e) {
                    # Continue
                  })
                }
              }
            }
          }
          
        }, error = function(e) {
          # Log error but don't fail completely
          warning(paste("Error in clean_traces:", e$message))
        })
        
        # Return results
        return(list(
          cleaned_traces,
          justification,
          predicted_value,
          confidence
        ))
      }
      
      extract_wikipedia_snippet <- function(text) {
        # Initialize result
        snippets <- c()
        
        tryCatch({
          # Robust pattern handling single/double quotes and escapes
          tool_pattern <- "ToolMessage\\(content=(?:'([^']*(?:\\\\'[^']*)*)'|\"([^\"]*(?:\\\\\"[^\"]*)*)\"), name=(?:'([wW]ikipedia)'|\"([wW]ikipedia)\").*?\\)"
          
          # Find all matches
          tool_matches <- gregexpr(tool_pattern, text, perl = TRUE)[[1]]
          
          if (tool_matches[1] != -1) {
            # Extract match texts
            match_texts <- regmatches(text, list(tool_matches))[[1]]
            
            for (match_text in match_texts) {
              # Extract components
              content_match <- regmatches(match_text, regexec(tool_pattern, match_text, perl = TRUE))[[1]]
              
              # Content in position 2 or 3
              content <- ifelse(nchar(content_match[2]) > 0, content_match[2], content_match[3])
              
              # Tool name in position 4 or 5, but we already matched [wW]ikipedia
              
              # Clean content
              if (!is.na(content) && nchar(content) > 0) {
                content <- gsub("\\\\n", " ", content)
                content <- gsub("\\\\'", "'", content)
                content <- gsub('\\\\"', '"', content)
                content <- gsub("\\\\\\\\", "\\\\", content)
                
                # Only add if it's a valid Wikipedia result
                if ((grepl("Page:", content) || grepl("Summary:", content)) && 
                    !grepl("No good Wikipedia Search Result", content, ignore.case = TRUE)) {
                  cleaned <- trimws(gsub("\\s+", " ", content))
                  if (nchar(cleaned) > 0) {
                    snippets <- c(snippets, cleaned)
                  }
                }
              }
            }
          }
        }, error = function(e) {
          warning(paste("Error in extract_wikipedia_snippet:", e$message))
        })
        
        # Return all valid snippets
        if (length(snippets) == 0) {
          return(character(0))
        }
        
        return(snippets)
      }
      
      extract_sources_by_number <- function(text) {
        # List to store content for each source number
        source_contents <- list()
        
        # Robust pattern handling single/double quotes and escapes for Search tool
        tool_pattern <- "ToolMessage\\(content=(?:'([^']*(?:\\\\'[^']*)*)'|\"([^\"]*(?:\\\\\"[^\"]*)*)\"), name=(?:'([sS]earch)'|\"([sS]earch)\").*?\\)"
        
        # Find all matches
        tool_matches <- gregexpr(tool_pattern, text, perl = TRUE)[[1]]
        
        if (tool_matches[1] != -1) {
          # Extract match texts
          match_texts <- regmatches(text, list(tool_matches))[[1]]
          
          for (match_text in match_texts) {
            # Extract components
            content_match <- regmatches(match_text, regexec(tool_pattern, match_text, perl = TRUE))[[1]]
            
            # Content in position 2 or 3
            content <- ifelse(nchar(content_match[2]) > 0, content_match[2], content_match[3])
            
            # Clean escape sequences in content
            if (!is.na(content) && nchar(content) > 0) {
              content <- gsub("\\\\n", " ", content)
              content <- gsub("\\\\'", "'", content)
              content <- gsub('\\\\"', '"', content)
              content <- gsub("\\\\\\\\", "\\\\", content)
              
              # Extract individual sources with their numbers - enable dotall for multiline
              source_pattern <- "(?s)__START_OF_SOURCE (\\d+)__ <CONTENT>(.*?)</CONTENT>.*?__END_OF_SOURCE.*?__"
              source_matches <- gregexpr(source_pattern, content, perl = TRUE)[[1]]
              
              if (source_matches[1] != -1) {
                source_match_texts <- regmatches(content, list(source_matches))[[1]]
                
                for (src_match in source_match_texts) {
                  # Extract source number using regexec
                  num_match <- regmatches(src_match, regexec("(?s)__START_OF_SOURCE (\\d+)__.*", src_match, perl = TRUE))[[1]]
                  source_num <- as.integer(num_match[2])
                  
                  # Extract content with dotall
                  cont_match <- regmatches(src_match, regexec("(?s)<CONTENT>(.*?)</CONTENT>", src_match, perl = TRUE))[[1]]
                  src_content <- cont_match[2]
                  
                  # Clean whitespace
                  cleaned_content <- trimws(gsub("\\s+", " ", src_content))
                  
                  if (nchar(cleaned_content) > 0) {
                    key <- as.character(source_num)
                    if (is.null(source_contents[[key]])) {
                      source_contents[[key]] <- c()
                    }
                    source_contents[[key]] <- c(source_contents[[key]], cleaned_content)
                  }
                }
              }
            }
          }
        }
        
        # Create output list with concatenated content for each source number
        result <- c()
        
        # Find the maximum source number
        if (length(source_contents) > 0) {
          max_source <- max(as.integer(names(source_contents)))
          
          # Create list with concatenated content for each source (1 to max)
          for (i in 1:max_source) {
            source_key <- as.character(i)
            if (source_key %in% names(source_contents)) {
              # Concatenate all content for this source number
              concatenated <- paste(source_contents[[source_key]], collapse = " ")
              result <- c(result, concatenated)
            } else {
              # Add empty string if source number is missing
              result <- c(result, "")
            }
          }
        }
        
        return(result)
      }
      
      extract_urls_by_source_old <- function(text) {
        # List to store URLs for each source number
        source_urls <- list()
        
        # Extract all Search tool messages
        tool_pattern <- "ToolMessage\\(content='(.*?)', name='Search'.*?\\)"
        search_matches <- regmatches(text, gregexpr(tool_pattern, text, perl = TRUE))[[1]]
        search_content_list <- gsub(tool_pattern, "\\1", search_matches, perl = TRUE)
        
        for (search_content in search_content_list) {
          # Extract individual sources with their numbers and URLs
          source_pattern <- "__START_OF_SOURCE (\\d+)__.*?<URL>(.*?)</URL>.*?__END_OF_SOURCE.*?__"
          source_matches <- regmatches(search_content, gregexpr(source_pattern, search_content, perl = TRUE))[[1]]
          
          for (match in source_matches) {
            # Extract source number and URL
            source_num <- as.integer(gsub(".*__START_OF_SOURCE (\\d+)__.*", "\\1", match, perl = TRUE))
            url <- gsub(".*<URL>(.*?)</URL>.*", "\\1", match, perl = TRUE)
            
            # Clean whitespace from URL
            cleaned_url <- trimws(url)
            
            if (nchar(cleaned_url) > 0) {
              source_urls[[as.character(source_num)]] <- cleaned_url
            }
          }
        }
        
        # Create output vector with URLs ordered by source number
        result <- c()
        
        # Find the maximum source number
        if (length(source_urls) > 0) {
          max_source <- max(as.integer(names(source_urls)))
          
          # Create vector with URLs for each source (1 to max)
          for (i in 1:max_source) {
            source_key <- as.character(i)
            if (source_key %in% names(source_urls)) {
              result <- c(result, source_urls[[source_key]])
            } else {
              # Add empty string if source number is missing
              result <- c(result, "")
            }
          }
        }
        
        return(result)
      }
      
      extract_urls_by_source <- function(text) {
        # List to store URLs for each source number
        source_urls <- list()
        
        # Robust pattern handling single/double quotes and escapes for Search tool
        tool_pattern <- "ToolMessage\\(content=(?:'([^']*(?:\\\\'[^']*)*)'|\"([^\"]*(?:\\\\\"[^\"]*)*)\"), name=(?:'([sS]earch)'|\"([sS]earch)\").*?\\)"
        
        # Find all matches
        tool_matches <- gregexpr(tool_pattern, text, perl = TRUE)[[1]]
        
        if (tool_matches[1] != -1) {
          # Extract match texts
          match_texts <- regmatches(text, list(tool_matches))[[1]]
          
          for (match_text in match_texts) {
            # Extract components
            content_match <- regmatches(match_text, regexec(tool_pattern, match_text, perl = TRUE))[[1]]
            
            # Content in position 2 or 3
            content <- ifelse(nchar(content_match[2]) > 0, content_match[2], content_match[3])
            
            # Clean escape sequences in content
            if (!is.na(content) && nchar(content) > 0) {
              content <- gsub("\\\\n", " ", content)
              content <- gsub("\\\\'", "'", content)
              content <- gsub('\\\\"', '"', content)
              content <- gsub("\\\\\\\\", "\\\\", content)
              
              # Extract individual sources with their numbers and URLs - enable dotall for multiline
              source_pattern <- "(?s)__START_OF_SOURCE (\\d+)__.*?<URL>(.*?)</URL>.*?__END_OF_SOURCE.*?__"
              source_matches <- gregexpr(source_pattern, content, perl = TRUE)[[1]]
              
              if (source_matches[1] != -1) {
                source_match_texts <- regmatches(content, list(source_matches))[[1]]
                
                for (src_match in source_match_texts) {
                  # Extract source number using regexec
                  num_match <- regmatches(src_match, regexec("(?s)__START_OF_SOURCE (\\d+)__.*", src_match, perl = TRUE))[[1]]
                  source_num <- as.integer(num_match[2])
                  
                  # Extract URL with dotall
                  url_match <- regmatches(src_match, regexec("(?s)<URL>(.*?)</URL>", src_match, perl = TRUE))[[1]]
                  url <- url_match[2]
                  
                  # Clean whitespace from URL
                  cleaned_url <- trimws(url)
                  
                  if (nchar(cleaned_url) > 0) {
                    key <- as.character(source_num)
                    # Since URLs are typically unique per source, but to handle multiples if any
                    if (is.null(source_urls[[key]])) {
                      source_urls[[key]] <- c()
                    }
                    source_urls[[key]] <- unique(c(source_urls[[key]], cleaned_url))
                  }
                }
              }
            }
          }
        }
        
        # Create output vector with URLs ordered by source number (concat if multiple)
        result <- c()
        
        # Find the maximum source number
        if (length(source_urls) > 0) {
          max_source <- max(as.integer(names(source_urls)))
          
          # Create vector with URLs for each source (1 to max)
          for (i in 1:max_source) {
            source_key <- as.character(i)
            if (source_key %in% names(source_urls)) {
              # Concatenate if multiple URLs (though unlikely), separated by space
              concatenated <- paste(source_urls[[source_key]], collapse = " ")
              result <- c(result, concatenated)
            } else {
              # Add empty string if source number is missing
              result <- c(result, "")
            }
          }
        }
        
        return(result)
      }
    }
  }
  
  # Initialize new columns
  df <- df %>%
    mutate(
      cleaned_traces = NA_character_,
      justification = NA_character_,
      predicted_value_cleaned = NA_real_,  # Assuming it's numeric based on function
      confidence = NA_character_
    )
  
  # test clean functions
  # clean_traces( df$raw_output[1] )
  # extract_sources_by_number( df$raw_output[1] )
  
  # Process each row with clean_traces function
  library(future.apply)
  plan(multisession(workers=10L))   
  cleaned_traces_list <- future_sapply(df$raw_output, function(v_){
    #cleaned_traces_list <- sapply(df$raw_output, function(v_){
    result <- clean_traces(v_)
    return(
      list( list("cleaned_traces" = paste(result[[1]],collapse = " "),
                 "justification" = result[[2]],
                 "predicted_value_cleaned" = result[[3]],
                 "confidence" = result[[4]]) )
    ) })
  df$cleaned_traces <- unlist(lapply(cleaned_traces_list,function(l_){l_$cleaned_traces}))
  df$justification <- unlist(lapply(cleaned_traces_list,function(l_){l_$justification}))
  df$predicted_value_cleaned <- unlist(lapply(cleaned_traces_list,function(l_){l_$predicted_value_cleaned}))
  df$confidence <- unlist(lapply(cleaned_traces_list,function(l_){l_$confidence}))
  
  # main sanity checks 
  {
    cat("Proportion of NA values in each cleaned column (indicates parsing success rate):\n")
    print( colMeans(is.na(df[,c("cleaned_traces","justification","predicted_value_cleaned","confidence")])))
    
    cat("Mean characters of rendered text (more usually better):")
    print( colMeans(apply(df[,c("cleaned_traces","justification","predicted_value_cleaned","confidence")],2,nchar),na.rm=T))
    
    cat("Plotting sorted lengths of cleaned_traces (plus 1) on log-log scale to visualize length distribution and detect outliers:\n")
    plot(1:nrow(df),1+sort(nchar(df$cleaned_traces)), log="xy")
    
    cat("Mean proportion of NA in cleaned_traces (missing parses):", mean(is.na(df$cleaned_traces)), "\n")
    cat("Mean proportion of empty cleaned_traces (failed extractions):", mean(nchar(df$cleaned_traces)==0), "\n")
    
    cat("Mean proportion of NA in justification (missing reasons):", mean(is.na(df$justification)), "\n")
    cat("Mean proportion of empty justifications (no content extracted):", mean(nchar(df$justification)==0, na.rm=TRUE), "\n")
    
    cat("Summary statistics of cleaned_traces lengths (checks variability and extremes):\n")
    print(summary(nchar(df$cleaned_traces)))
    
    cat("Example of a long cleaned_trace (length 994) to inspect content:\n")
    print(df$cleaned_traces[which(nchar(df$cleaned_traces) == 994)[1]])
    
    cat("Corresponding raw_output for the long cleaned_trace to compare parsing:\n")
    print(df$raw_output[which(nchar(df$cleaned_traces) == 994)[1]])
    
    cat("Minimum non-NA lengths in each cleaned column (flags very short or empty entries):\n")
    print(apply(apply(df[,c("cleaned_traces","justification","predicted_value_cleaned","confidence")],2,nchar),2,function(x){min(x,na.rm=TRUE)}))
    
    cat("Removing cleaned_traces_list and running garbage collection to free memory:\n")
    rm(cleaned_traces_list); gc()
  }
  
  # get wikipedia_df 
  wikipedia_df <- unlist(future_sapply(df$raw_output, function(ct) {
    s  <- extract_wikipedia_snippet(ct)
    if(length(s) == 0){ s <- NA_character_ }
    s <- paste(s,collapse=" ")
    return(s)
  },USE.NAMES=FALSE))
  df$wikipedia_snippet <- wikipedia_df
  
  # wiki sanities 
  {
    cat("Proportion of NA values in wikipedia_snippet (indicates parsing success rate):\n")
    print(mean(is.na(df$wikipedia_snippet)))
    
    cat("Mean characters of wikipedia_snippet (more usually better):\n")
    print(mean(nchar(df$wikipedia_snippet), na.rm=TRUE))
    
    print("Mean 0 snippets (indicates BLANK)")
    print(mean(nchar(df$wikipedia_snippet)==0, na.rm=TRUE))
    
    print("Mean 2 snippets (indicates NAs)")
    print(mean(nchar(df$wikipedia_snippet)==2, na.rm=TRUE))
    
    cat("Plotting sorted lengths of wikipedia_snippet (plus 1) on log-log scale to visualize length distribution and detect outliers:\n")
    plot(1:nrow(df), 1+sort(nchar(df$wikipedia_snippet)), log="xy", main="Sorted Lengths of wikipedia_snippet")
    
    cat("Mean proportion of empty wikipedia_snippet (failed extractions):\n")
    print(mean(nchar(df$wikipedia_snippet)==0, na.rm=TRUE))
    
    cat("Summary statistics of wikipedia_snippet lengths (checks variability and extremes):\n")
    print(summary(nchar(df$wikipedia_snippet)))
    
    cat("Example of a long wikipedia_snippet to inspect content (selecting one with length close to 90th percentile):\n")
    long_len <- quantile(nchar(df$wikipedia_snippet), 0.9, na.rm=TRUE)
    long_idx <- which.min(abs(nchar(df$wikipedia_snippet) - long_len))
    print(df$wikipedia_snippet[long_idx])
    
    cat("Corresponding raw_output for the long wikipedia_snippet to compare parsing:\n")
    print(df$raw_output[long_idx])
    
    cat("Minimum non-NA length in wikipedia_snippet (flags very short or empty entries):\n")
    print(min(nchar(df$wikipedia_snippet), na.rm=TRUE))
  }
  rm(wikipedia_df);capture.output(gc())
  
  # get sources df
  sources_df <- future_lapply(df$raw_output, function(ct) {
    if (length(ct) > 0) {
      s  <- extract_sources_by_number(ct)
      n  <- length(s)
      if (n < 10){ s <- c(s, rep(NA_character_, 10 - n)) }
      return(s[1:10])
    } else {
      return(rep(NA_character_, 10))
    }})
  
  # Add columns source_1, ..., source_10
  sources_df <- do.call(
    rbind, lapply(sources_df, function(s) {
      # ensure it's a named vector
      names(s) <- paste0("search_snippet_", seq_along(s))
      return( s )  })
  ) |> as.data.frame(stringsAsFactors = FALSE)
  df <- bind_cols(df, sources_df)
  
  # sources sanities 
  {
    cat("Proportion of NA values in each search_snippet column (indicates parsing success rate):\n")
    print(colMeans(is.na(df[,paste0("search_snippet_", 1:10)])))
    
    # look at data 
    df[1,paste0("search_snippet_", 1)]; df[1,paste0("search_snippet_", 10)]
    df[100,paste0("search_snippet_", 1)]; df[100,paste0("search_snippet_", 10)]
    
    cat("Proportion of len 2 values:\n")
    print(colMeans(apply(df[,paste0("search_snippet_", 1:10)],2,nchar)==2,na.rm=T))
    
    cat("Mean characters of search_snippet columns (more usually better):\n")
    print(colMeans(apply(df[,paste0("search_snippet_", 1:10)], 2, nchar), na.rm=TRUE))
    
    cat("Plotting sorted lengths of search_snippet_1 (plus 1) on log-log scale to visualize length distribution and detect outliers:\n")
    #plot(1:nrow(df), 1+sort(nchar(df$search_snippet_1)), log="xy", main="Sorted Lengths of search_snippet_1")
    
    cat("Mean proportion of NA in search_snippet_1 (missing parses):\n")
    print(mean(is.na(df$search_snippet_1)))
    
    cat("Mean proportion of empty search_snippet_1 (failed extractions):\n")
    print(mean(nchar(df$search_snippet_1)==0, na.rm=TRUE))
    
    cat("Summary statistics of search_snippet_1 lengths (checks variability and extremes):\n")
    print(summary(nchar(df$search_snippet_1)))
    
    cat("Example of a long search_snippet_1 to inspect content (selecting one with length close to 90th percentile):\n")
    long_len <- quantile(nchar(df$search_snippet_1), 0.9, na.rm=TRUE)
    long_idx <- which.min(abs(nchar(df$search_snippet_1) - long_len))
    print(df$search_snippet_1[long_idx])
    
    cat("Corresponding raw_output for the long search_snippet_1 to compare parsing:\n")
    print(df$raw_output[long_idx])
    
    cat("Minimum non-NA lengths in search_snippet columns (flags very short or empty entries):\n")
    print(apply(apply(df[,paste0("search_snippet_", 1:10)], 2, nchar), 2, min, na.rm=TRUE))
  }
  rm(sources_df);gc()
  
  # Add columns search_url_1, ..., search_url_10
  urls_df <- future_lapply(df$raw_output, function(ct) {
    if (length(ct) > 0) {
      s  <- extract_urls_by_source(ct)
      n  <- length(s)
      if (n < 10){ s <- c(s, rep(NA_character_, 10 - n)) }
      return(s[1:10])
    } else {
      return(rep(NA_character_, 10))
    }})
  urls_df <- do.call(
    rbind, lapply(urls_df, function(s) {
      # ensure it's a named vector
      names(s) <- paste0("search_url_", seq_along(s))
      return( s )  })
  ) |> as.data.frame(stringsAsFactors = FALSE)
  df <- bind_cols(df, urls_df)
  
  # URL sanities 
  {
    cat("Proportion of NA values in each search_url column (indicates parsing success rate):\n")
    print(colMeans(is.na(df[,paste0("search_url_", 1:10)])))
    
    # look at data 
    df[1,paste0("search_url_", 1)]; df[1,paste0("search_url_", 10)]
    df[100,paste0("search_url_", 1)]; df[100,paste0("search_url_", 10)]
    
    cat("Mean characters of search_url columns (more usually better):\n")
    print(colMeans(apply(df[,paste0("search_url_", 1:10)], 2, nchar), na.rm=TRUE))
    
    cat("Plotting sorted lengths of search_url_1 (plus 1) on log-log scale to visualize length distribution and detect outliers:\n")
    #plot(1:nrow(df), 1+sort(nchar(df$search_url_1)), log="xy", main="Sorted Lengths of search_url_1")
    
    cat("Mean proportion of NA in search_url_1 (missing parses):\n")
    print(mean(is.na(df$search_url_1)))
    
    cat("Mean proportion of empty search_url_1 (failed extractions):\n")
    print(mean(nchar(df$search_url_1)==0, na.rm=TRUE))
    
    cat("Summary statistics of search_url_1 lengths (checks variability and extremes):\n")
    print(summary(nchar(df$search_url_1)))
    
    cat("Example of a long search_url_1 to inspect content (selecting one with length close to 90th percentile):\n")
    long_len <- quantile(nchar(df$search_url_1), 0.9, na.rm=TRUE)
    long_idx <- which.min(abs(nchar(df$search_url_1) - long_len))
    print(df$search_url_1[long_idx])
    
    cat("Minimum non-NA lengths in search_url columns (flags very short or empty entries):\n")
    print(apply(apply(df[,paste0("search_url_", 1:10)], 2, nchar), 2, min, na.rm=TRUE))
    
    cat("Proportion of valid URLs in search_url_1 (basic regex check for http(s)://):\n")
    print(mean(grepl("^https?://", df$search_url_1, ignore.case=TRUE), na.rm=TRUE))
  }
  rm(urls_df);gc()
  plan(sequential)
  
  # analyze overall missing patterns 
  round(sort(colMeans(is.na(df))),3)

}