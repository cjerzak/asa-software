#' Normalize Tor registry settings
#'
#' Shared helper used by tor_options() and configure_tor_registry() to keep
#' defaults and type coercions consistent.
#'
#' @param registry_path Registry path (NULL/"" for default)
#' @param enable Enable/disable registry tracking
#' @param bad_ttl Seconds to keep a bad/tainted exit before reuse
#' @param good_ttl Seconds to treat an exit as good before refreshing
#' @param overuse_threshold Maximum recent uses before treating an exit as overloaded
#' @param overuse_decay Window (seconds) for counting recent uses
#' @param max_rotation_attempts Maximum rotations to find a clean exit
#' @param ip_cache_ttl Seconds to cache exit IP lookups
#' @param create_parent_for_custom If TRUE, create parent dir for custom paths
#' @return List with normalized fields (including resolved registry_path)
#' @keywords internal
.normalize_tor_registry_options <- function(registry_path = NULL,
                                           enable = ASA_TOR_REGISTRY_ENABLED,
                                           bad_ttl = ASA_TOR_BAD_TTL,
                                           good_ttl = ASA_TOR_GOOD_TTL,
                                           overuse_threshold = ASA_TOR_OVERUSE_THRESHOLD,
                                           overuse_decay = ASA_TOR_OVERUSE_DECAY,
                                           max_rotation_attempts = ASA_TOR_MAX_ROTATION_ATTEMPTS,
                                           ip_cache_ttl = ASA_TOR_IP_CACHE_TTL,
                                           create_parent_for_custom = FALSE) {
  if (is.null(registry_path) || registry_path == "") {
    base_dir <- tools::R_user_dir("asa", which = "cache")
    dir.create(base_dir, recursive = TRUE, showWarnings = FALSE)
    registry_path <- file.path(base_dir, "tor_exit_registry.sqlite")
  } else if (isTRUE(create_parent_for_custom)) {
    dir.create(dirname(registry_path), recursive = TRUE, showWarnings = FALSE)
  }

  list(
    registry_path = registry_path,
    enable = isTRUE(enable),
    bad_ttl = as.numeric(bad_ttl),
    good_ttl = as.numeric(good_ttl),
    overuse_threshold = as.integer(overuse_threshold),
    overuse_decay = as.numeric(overuse_decay),
    max_rotation_attempts = as.integer(max_rotation_attempts),
    ip_cache_ttl = as.numeric(ip_cache_ttl)
  )
}

