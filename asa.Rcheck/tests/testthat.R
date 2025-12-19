# This file is part of the standard setup for testthat.
# It is recommended that you do not modify it.
#
# Where should you do your testing?
# Learn more about testing at https://r-pkgs.org/tests.html
options(error = NULL)
#setwd("~/Documents/asa-software/")
library(testthat)
library(asa)

test_check("asa")

# test_check("asa") # - designed for R CMD check, not interactive use
devtools::test("~/Documents/asa-software/asa/")#  - use this for interactive testing
