# Script: LLM_Helpers.R
{
  json_escape <- function(x) {
    x <- gsub("\\\\", "\\\\\\\\", x, perl = TRUE)  # escape backslashes
    x <- gsub("\"", "\\\\\"", x, perl = TRUE)      # escape double quotes
    x
  }
  CleanText <- function(name){
    name <- textutils::HTMLdecode(name)
    return(name)
  }
  fix_encoding <- function(df){ 
    if ("person_name" %in% colnames(df) && is.character(df$person_name)) {
      # Extract sample text
      sample_text <- df$person_name[1:min(nrow(df), 10)] 
      sample_text <- sample_text[!is.na(sample_text) & sample_text != ""]
      if (length(sample_text) > 0) { 
        # Guess encoding
        temp_file <- tempfile() 
        writeLines(sample_text, temp_file, useBytes = TRUE) 
        encoding_guess <- readr::guess_encoding(temp_file)$encoding[1] 
        unlink(temp_file)
        if (!is.na(encoding_guess) && encoding_guess != "UTF-8") { 
          df$person_name <- iconv(df$person_name, from = "UTF-8", 
                                  to = encoding_guess, 
                                  sub = "byte")
        }
      }
    }
    return( df ) 
  }
}
