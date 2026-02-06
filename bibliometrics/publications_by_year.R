#!/usr/bin/env Rscript
# publications_by_year.R
# Demo script: publications per journal per year
#
# Requires: DBI, RSQLite, dplyr, dbplyr, ggplot2, tidyr, scales, ggrepel
# Install with: install.packages(c("DBI", "RSQLite", "dplyr", "dbplyr", "ggplot2", "tidyr", "scales", "ggrepel"))

library(DBI)
library(RSQLite)
library(dplyr)
library(dbplyr)
library(ggplot2)
library(ggrepel)

# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------

# Connect to linglitter.db (adjust path if running from different directory)
db_path <- file.path(dirname(getwd()), "linglitter.db")
if (!file.exists(db_path)) {
  db_path <- "linglitter.db"  # fallback: current directory
}
if (!file.exists(db_path)) {
  stop("Database not found. Run from project root or bibliometrics/ directory.")
}

con <- dbConnect(SQLite(), db_path)
cat("Connected to:", db_path, "\n")

# ---------------------------------------------------------------------------
# Query data using dplyr/dbplyr
# ---------------------------------------------------------------------------

# Create a lazy table reference (no data loaded yet)
articles_tbl <- tbl(con, "articles")

# Count publications per journal per year
# This builds a SQL query behind the scenes
pubs_by_year <- articles_tbl %>%
  filter(!is.na(year), !is.na(journal), year >= 2005, year <= 2025) %>%
  count(journal, year) %>%
  arrange(journal, year) %>%
  collect()  # Execute query and fetch results

cat("\nPublications per journal per year:\n")
print(pubs_by_year, n = 20)

# Get year range from data
year_range <- range(pubs_by_year$year, na.rm = TRUE)
cat("\nYear range:", year_range[1], "-", year_range[2], "\n")

# Get list of journals
journals <- unique(pubs_by_year$journal)
cat("Journals:", length(journals), "\n")

# ---------------------------------------------------------------------------
# Summary statistics
# ---------------------------------------------------------------------------

# Total publications per journal
totals <- pubs_by_year %>%
  group_by(journal) %>%
  summarise(total = sum(n), .groups = "drop") %>%
  arrange(desc(total))

cat("\nTotal publications per journal:\n")
print(totals, n = 20)
