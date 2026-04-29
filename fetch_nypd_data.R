# ─────────────────────────────────────────────────────────────────────────────
# NYPD Crime Complaint Data Fetcher
# Run this script each quarter to refresh crime_data.json.
# From RStudio: open this file, paste your token below, click Source
#
# Requires: install.packages(c("httr", "jsonlite"))
# ─────────────────────────────────────────────────────────────────────────────

library(httr)
library(jsonlite)

# !! PASTE YOUR FREE APP TOKEN BETWEEN THE QUOTES BELOW !!
# Get one at: data.cityofnewyork.us -> Sign In -> Developer Settings -> Create New App Token
APP_TOKEN <- ""

HISTORIC_ENDPOINT <- "https://data.cityofnewyork.us/resource/qgea-i56i.json"
YTD_ENDPOINT      <- "https://data.cityofnewyork.us/resource/5uac-w243.json"

# ── SET THESE MANUALLY EACH QUARTER ─────────────────────────────────────────
# Update these two lines when new data becomes available.
# current_year    = the most recent year with data (e.g. 2025)
# current_quarter = the most recent quarter available (1, 2, 3, or 4)
current_year    <- 2026
current_quarter <- 1
# ─────────────────────────────────────────────────────────────────────────────

ytd_end_month <- current_quarter * 3
today         <- Sys.Date()

cat(sprintf("Data set to: %d Q%d  |  YTD through month %d  |  (today is %s)\n",
            current_year, current_quarter, ytd_end_month, today))

# ── Crime groups ──────────────────────────────────────────────────────────────

CRIME_GROUPS <- list(
  "Major crimes" = c(
    "FELONY ASSAULT", "ROBBERY", "BURGLARY",
    "GRAND LARCENY", "GRAND LARCENY OF MOTOR VEHICLE",
    "RAPE", "MURDER & NON-NEGL. MANSLAUGHTER"
  ),
  "Major violent crimes" = c(
    "FELONY ASSAULT", "ROBBERY", "RAPE", "MURDER & NON-NEGL. MANSLAUGHTER"
  ),
  "Major property crimes" = c(
    "BURGLARY", "GRAND LARCENY", "GRAND LARCENY OF MOTOR VEHICLE"
  ),
  "All assaults" = c(
    "FELONY ASSAULT", "ASSAULT 3 & RELATED OFFENSES",
    "HARRASSMENT 2", "HARASSMENT 2",
    "MENACING,RECKLESS ENDANGERMENT", "OFFENSES AGAINST THE PERSON"
  )
)

BOROUGH_MAP <- c(
  "PATROL BORO BRONX"         = "Bronx",
  "PATROL BORO BKLYN NORTH"   = "Brooklyn",
  "PATROL BORO BKLYN SOUTH"   = "Brooklyn",
  "PATROL BORO MAN NORTH"     = "Manhattan",
  "PATROL BORO MAN SOUTH"     = "Manhattan",
  "PATROL BORO QUEENS NORTH"  = "Queens",
  "PATROL BORO QUEENS SOUTH"  = "Queens",
  "PATROL BORO STATEN ISLAND" = "Staten Island"
)

# ── Socrata query with pagination ─────────────────────────────────────────────
# Fetches ALL rows by looping with $offset until we get fewer than page_size back

socrata_query_all <- function(endpoint, soql, page_size = 50000) {
  all_results <- list()
  offset <- 0

  repeat {
    # Append LIMIT and OFFSET to the SoQL query
    paged_soql <- sprintf("%s LIMIT %d OFFSET %d", soql, page_size, offset)

    qparams <- list(`$query` = paged_soql)
    if (nchar(APP_TOKEN) > 0) qparams[["$$app_token"]] <- APP_TOKEN

    resp <- GET(
      url   = endpoint,
      query = qparams,
      add_headers(Accept = "application/json"),
      timeout(120)
    )
    if (http_error(resp)) {
      stop(paste("API error:", status_code(resp), content(resp, "text", encoding = "UTF-8")))
    }

    batch <- fromJSON(content(resp, "text", encoding = "UTF-8"), flatten = TRUE)

    if (is.null(batch) || !is.data.frame(batch) || nrow(batch) == 0) break

    all_results[[length(all_results) + 1]] <- batch
    if (nrow(batch) < page_size) break   # last page
    offset <- offset + page_size
    Sys.sleep(0.2)
  }

  if (length(all_results) == 0) return(NULL)
  do.call(rbind, all_results)
}

# ── Build SoQL for one year ───────────────────────────────────────────────────

build_soql <- function(year, ytd_month_end) {
  loc_expr <- paste0(
    "CASE ",
    "WHEN jurisdiction_code = 1 THEN 'subway' ",
    "WHEN jurisdiction_code = 2 THEN 'housing' ",
    "ELSE 'other' END"
  )
  q_expr <- paste0(
    "CASE ",
    "WHEN date_extract_m(rpt_dt) <= 3 THEN 'Q1' ",
    "WHEN date_extract_m(rpt_dt) <= 6 THEN 'Q2' ",
    "WHEN date_extract_m(rpt_dt) <= 9 THEN 'Q3' ",
    "ELSE 'Q4' END"
  )
  sprintf(
    "SELECT date_extract_y(rpt_dt) AS yr, %s AS quarter,
            ofns_desc, patrol_boro, addr_pct_cd, %s AS loc_type, COUNT(*) AS n
     WHERE  rpt_dt >= '%d-01-01T00:00:00'
       AND  rpt_dt <  '%d-01-01T00:00:00'
       AND  date_extract_m(rpt_dt) <= %d
       AND  ofns_desc IS NOT NULL
     GROUP BY yr, quarter, ofns_desc, patrol_boro, addr_pct_cd, loc_type",
    q_expr, loc_expr,
    year, year + 1, ytd_month_end
  )
}

# ── Fetch all years ───────────────────────────────────────────────────────────

all_rows <- list()

# Historic dataset: 2006 through 2025 (2025 moved to historic as of Q1 2026)
hist_end <- 2025
cat(sprintf("\nFetching historic data year by year (2006-%d)...\n", hist_end))

for (yr in 2006:hist_end) {
  cat(sprintf("  Fetching %d... ", yr))
  result <- tryCatch(
    socrata_query_all(HISTORIC_ENDPOINT, build_soql(yr, ytd_end_month)),
    error = function(e) { cat(sprintf("ERROR: %s\n", e$message)); NULL }
  )
  if (!is.null(result) && is.data.frame(result) && nrow(result) > 0) {
    all_rows[[length(all_rows) + 1]] <- result
    cat(sprintf("%d rows\n", nrow(result)))
  } else {
    cat("no data\n")
  }
  Sys.sleep(0.3)
}

# YTD dataset: current year (and any years not yet in historic)
# hist_end is hardcoded to 2024 — update to 2025 once that moves to historic
ytd_years <- unique(c(current_year))
# Add prior year if not yet in historic (2025 moved to historic, so only 2026+ needed)
if (current_year > 2026) ytd_years <- unique(c(current_year - 1, ytd_years))
cat(sprintf("\nFetching YTD data (%s)...\n", paste(ytd_years, collapse=" and ")))
for (yr in ytd_years) {
  cat(sprintf("  Fetching %d... ", yr))
  result <- tryCatch(
    socrata_query_all(YTD_ENDPOINT, build_soql(yr, ytd_end_month)),
    error = function(e) { cat(sprintf("ERROR: %s\n", e$message)); NULL }
  )
  if (!is.null(result) && is.data.frame(result) && nrow(result) > 0) {
    all_rows[[length(all_rows) + 1]] <- result
    cat(sprintf("%d rows\n", nrow(result)))
  } else {
    cat("no data\n")
  }
  Sys.sleep(0.3)
}

all_rows <- do.call(rbind, all_rows)
cat(sprintf("\nTotal rows fetched: %d\n", nrow(all_rows)))

# ── Process ───────────────────────────────────────────────────────────────────

cat("Processing...\n")

all_rows$ofns_desc   <- toupper(trimws(as.character(all_rows$ofns_desc)))
all_rows$patrol_boro <- toupper(trimws(as.character(all_rows$patrol_boro)))
all_rows$loc_type    <- as.character(all_rows$loc_type)
all_rows$quarter     <- as.character(all_rows$quarter)
all_rows$yr          <- as.character(as.integer(as.numeric(all_rows$yr)))
all_rows$n           <- as.integer(as.numeric(all_rows$n))
all_rows$precinct    <- as.integer(as.numeric(as.character(all_rows$addr_pct_cd)))

all_rows <- all_rows[
  !is.na(all_rows$ofns_desc) & all_rows$ofns_desc != "" &
  !is.na(all_rows$n) & all_rows$n > 0 &
  !is.na(all_rows$yr) & all_rows$yr != "0", ]

all_rows$borough <- BOROUGH_MAP[all_rows$patrol_boro]
all_rows$borough[is.na(all_rows$borough)] <- "Other"

# ── Filter: only keep crimes with > 100 incidents in the current year ─────────
# (We still keep ALL crimes for group calculations — filter only affects individual dropdown)

ytd_year_str <- as.character(current_year)
current_yr_rows <- all_rows[all_rows$yr == ytd_year_str, ]
crime_totals_current <- aggregate(n ~ ofns_desc, data = current_yr_rows, FUN = sum)
crimes_over_100 <- crime_totals_current$ofns_desc[crime_totals_current$n > 100]
cat(sprintf("  %d crime types have > 100 incidents in %d (others hidden from dropdown)
",
            length(crimes_over_100), current_year))

all_crime_types <- sort(crimes_over_100)

# The 7 major individual crimes — always shown even if under threshold
MAJOR_SEVEN <- c(
  "MURDER & NON-NEGL. MANSLAUGHTER", "RAPE", "ROBBERY",
  "FELONY ASSAULT", "BURGLARY",
  "GRAND LARCENY", "GRAND LARCENY OF MOTOR VEHICLE"
)

# Sorted dropdown: major 7 first, then rest alphabetically
major_in_list  <- sort(intersect(MAJOR_SEVEN, all_crime_types))
others_in_list <- sort(setdiff(all_crime_types, MAJOR_SEVEN))
all_crime_types_ordered <- c(major_in_list, others_in_list)

# Expand to three location buckets (use ALL rows for group totals)
rows_c <- all_rows; rows_c$bucket <- "citywide"
rows_s <- all_rows[all_rows$loc_type == "subway",  ]; rows_s$bucket <- "subway"
rows_h <- all_rows[all_rows$loc_type == "housing", ]; rows_h$bucket <- "housing"
expanded <- rbind(rows_c, rows_s, rows_h)

# Aggregate by borough
agg <- aggregate(n ~ ofns_desc + bucket + borough + yr + quarter,
                 data = expanded[expanded$borough != "Other", ], FUN = sum)

# Add All boroughs
agg_all <- aggregate(n ~ ofns_desc + bucket + yr + quarter, data = expanded, FUN = sum)
agg_all$borough <- "All boroughs"
agg <- rbind(agg, agg_all[, names(agg)])

# Add "All crime" group — sum of every offense
cat("  Building 'All crime' group (all offenses)...
")
all_crime_group <- aggregate(n ~ bucket + borough + yr + quarter, data = agg[agg$ofns_desc %in% unique(all_rows$ofns_desc), ], FUN = sum)
all_crime_group_all <- aggregate(n ~ bucket + yr + quarter, data = expanded, FUN = sum)
all_crime_group_all$borough <- "All boroughs"
all_crime_combined <- rbind(
  aggregate(n ~ bucket + borough + yr + quarter, data = expanded[expanded$borough != "Other", ], FUN = sum),
  all_crime_group_all[, c("bucket","borough","yr","quarter","n")]
)
all_crime_combined$ofns_desc <- "All crime"
agg <- rbind(agg, all_crime_combined[, names(agg)])

# Add other crime groups
group_rows <- list()
for (gname in names(CRIME_GROUPS)) {
  members <- CRIME_GROUPS[[gname]]
  sub     <- agg[agg$ofns_desc %in% members, ]
  if (nrow(sub) == 0) next
  g <- aggregate(n ~ bucket + borough + yr + quarter, data = sub, FUN = sum)
  g$ofns_desc <- gname
  group_rows[[gname]] <- g[, names(agg)]
}
if (length(group_rows) > 0) agg <- rbind(agg, do.call(rbind, group_rows))

all_crime_types <- all_crime_types_ordered

# ── Precinct-level aggregation (citywide only for map) ───────────────────────
cat("Building precinct aggregation...\n")

# Precinct aggregation — one per location type
# loc_type values: "other" = citywide, "subway", "housing"
build_pct_data <- function(rows_subset) {
  if (nrow(rows_subset) == 0) return(list())

  rows_subset$pct_key <- paste0("pct_", rows_subset$precinct)
  agg <- aggregate(n ~ ofns_desc + pct_key + yr + quarter,
                   data = rows_subset, FUN = sum)

  # Crime groups
  group_rows <- list()
  for (gname in names(CRIME_GROUPS)) {
    members <- CRIME_GROUPS[[gname]]
    sub_p   <- agg[agg$ofns_desc %in% members, ]
    if (nrow(sub_p) == 0) next
    g_p <- aggregate(n ~ pct_key + yr + quarter, data = sub_p, FUN = sum)
    g_p$ofns_desc <- gname
    group_rows[[gname]] <- g_p[, c("ofns_desc","pct_key","yr","quarter","n")]
  }
  all_c <- aggregate(n ~ pct_key + yr + quarter, data = agg, FUN = sum)
  all_c$ofns_desc <- "All crime"

  final <- rbind(
    agg[, c("ofns_desc","pct_key","yr","quarter","n")],
    if (length(group_rows) > 0) do.call(rbind, group_rows) else NULL,
    all_c[, c("ofns_desc","pct_key","yr","quarter","n")]
  )

  # Build nested list
  out <- list()
  for (i in seq_len(nrow(final))) {
    cr <- final$ofns_desc[i]; pk <- final$pct_key[i]
    yr <- final$yr[i];        qt <- final$quarter[i]
    n  <- final$n[i]
    if (is.null(out[[cr]]))             out[[cr]]             <- list()
    if (is.null(out[[cr]][[pk]]))       out[[cr]][[pk]]       <- list()
    if (is.null(out[[cr]][[pk]][[yr]])) out[[cr]][[pk]][[yr]] <- list()
    out[[cr]][[pk]][[yr]][[qt]] <- n
  }
  out
}

rows_with_pct <- all_rows[!is.na(all_rows$precinct) & all_rows$precinct > 0, ]

cat("  Building citywide precinct data...\n")
pct_citywide <- build_pct_data(rows_with_pct[rows_with_pct$loc_type == "other", ])
cat(sprintf("    %d precincts\n", length(pct_citywide[["All crime"]])))

cat("  Building subway precinct data...\n")
pct_subway <- build_pct_data(rows_with_pct[rows_with_pct$loc_type == "subway", ])
cat(sprintf("    %d precincts\n", length(pct_subway[["All crime"]])))

cat("  Building housing precinct data...\n")
pct_housing <- build_pct_data(rows_with_pct[rows_with_pct$loc_type == "housing", ])
cat(sprintf("    %d precincts\n", length(pct_housing[["All crime"]])))

pct_data_out <- list(
  citywide = pct_citywide,
  subway   = pct_subway,
  housing  = pct_housing
)

# ── Subway ridership ─────────────────────────────────────────────────────────
# Pre-2018: hardcoded annual totals from MTA official reports + NYU Wagner research
# Source: MTA Annual Ridership Reports; NYU Wagner "State of Subway Ridership" (2017)
# Distributed evenly across 4 quarters as approximation
# 2018+: fetched quarterly from MTA Open Data API (data.ny.gov xfre-bxip)
cat("\nBuilding subway ridership table...\n")

ANNUAL_RIDERSHIP_PRE2018 <- c(
  "2006" = 1500247828,
  "2007" = 1563703626,
  "2008" = 1625069716,
  "2009" = 1581263428,
  "2010" = 1605802398,
  "2011" = 1640643036,
  "2012" = 1654569900,
  "2013" = 1707539429,
  "2014" = 1751221791,
  "2015" = 1762527050,
  "2016" = 1757153000,  # MTA Annual Report
  "2017" = 1727366000   # MTA Annual Report
)

subway_ridership <- list()

# Populate pre-2018 by dividing annual total evenly across 4 quarters
for (yr_name in names(ANNUAL_RIDERSHIP_PRE2018)) {
  annual <- ANNUAL_RIDERSHIP_PRE2018[[yr_name]]
  subway_ridership[[yr_name]] <- list(
    Q1 = round(annual * 0.24),  # Q1 slightly lower (winter)
    Q2 = round(annual * 0.26),  # Q2/Q3 higher (summer)
    Q3 = round(annual * 0.27),
    Q4 = round(annual * 0.23)   # Q4 slightly lower
  )
}
cat(sprintf("  Pre-2018 ridership hardcoded for %d years\n", length(ANNUAL_RIDERSHIP_PRE2018)))

# Fetch 2018+ from API
mta_resp <- tryCatch({
  GET("https://data.ny.gov/resource/xfre-bxip.json",
      query = list(
        `$where`  = "agency='Subway'",
        `$select` = "month,ridership",
        `$order`  = "month ASC",
        `$limit`  = "5000"
      ),
      timeout(60))
}, error = function(e) { cat(sprintf("  WARNING: Could not fetch MTA API: %s\n", e$message)); NULL })

if (!is.null(mta_resp) && !http_error(mta_resp)) {
  mta_data <- fromJSON(content(mta_resp, "text", encoding = "UTF-8"), flatten = TRUE)
  if (is.data.frame(mta_data) && nrow(mta_data) > 0) {
    mta_data$yr  <- as.integer(substr(as.character(mta_data$month), 1, 4))
    mta_data$mo  <- as.integer(substr(as.character(mta_data$month), 6, 7))
    mta_data$ridership <- as.numeric(mta_data$ridership)
    mta_data <- mta_data[!is.na(mta_data$yr) & !is.na(mta_data$ridership) & mta_data$yr >= 2018, ]
    mta_data$quarter <- ifelse(mta_data$mo <= 3, "Q1",
                        ifelse(mta_data$mo <= 6, "Q2",
                        ifelse(mta_data$mo <= 9, "Q3", "Q4")))
    mta_qtr <- aggregate(ridership ~ yr + quarter, data = mta_data, FUN = sum)
    for (i in seq_len(nrow(mta_qtr))) {
      yr_key <- as.character(mta_qtr$yr[i])
      qt_key <- mta_qtr$quarter[i]
      if (is.null(subway_ridership[[yr_key]])) subway_ridership[[yr_key]] <- list()
      subway_ridership[[yr_key]][[qt_key]] <- round(mta_qtr$ridership[i])
    }
    cat(sprintf("  API ridership loaded for 2018-%d\n", max(mta_qtr$yr)))
  }
} else {
  cat("  WARNING: MTA API fetch failed — using pre-2018 data only\n")
}
cat(sprintf("  Total ridership years loaded: %s\n",
            paste(sort(names(subway_ridership)), collapse=", ")))

# ── NYC + borough population estimates (Census ACS, per 100k) ────────────────
# Annual estimates — update every few years with new ACS releases
# Source: NYC Department of City Planning / Census Bureau
NYC_POP <- list(
  "2006"=list(nyc=8214426, Bronx=1364566, Brooklyn=2505040, Manhattan=1596843, Queens=2270338, `Staten Island`=472639),
  "2007"=list(nyc=8274527, Bronx=1384115, Brooklyn=2528799, Manhattan=1604776, Queens=2285116, `Staten Island`=471721),
  "2008"=list(nyc=8333346, Bronx=1397751, Brooklyn=2551026, Manhattan=1621279, Queens=2290514, `Staten Island`=472776),
  "2009"=list(nyc=8391881, Bronx=1410657, Brooklyn=2572671, Manhattan=1634877, Queens=2303200, `Staten Island`=470476),
  "2010"=list(nyc=8175133, Bronx=1385108, Brooklyn=2504700, Manhattan=1585873, Queens=2230722, `Staten Island`=468730),
  "2011"=list(nyc=8244910, Bronx=1398975, Brooklyn=2532645, Manhattan=1601948, Queens=2237644, `Staten Island`=473698),
  "2012"=list(nyc=8336697, Bronx=1420695, Brooklyn=2565635, Manhattan=1619268, Queens=2253615, `Staten Island`=477484),
  "2013"=list(nyc=8405837, Bronx=1432132, Brooklyn=2595259, Manhattan=1626159, Queens=2272771, `Staten Island`=479516),
  "2014"=list(nyc=8491079, Bronx=1446788, Brooklyn=2629150, Manhattan=1636268, Queens=2296177, `Staten Island`=482696),
  "2015"=list(nyc=8550405, Bronx=1455720, Brooklyn=2649049, Manhattan=1643734, Queens=2312476, `Staten Island`=476426),
  "2016"=list(nyc=8537673, Bronx=1455919, Brooklyn=2641052, Manhattan=1644518, Queens=2321580, `Staten Island`=474604),
  "2017"=list(nyc=8622698, Bronx=1471160, Brooklyn=2678884, Manhattan=1664727, Queens=2339150, `Staten Island`=476777),
  "2018"=list(nyc=8398748, Bronx=1432132, Brooklyn=2600747, Manhattan=1628706, Queens=2278906, `Staten Island`=476253),
  "2019"=list(nyc=8336817, Bronx=1418207, Brooklyn=2576771, Manhattan=1628701, Queens=2253858, `Staten Island`=475280),
  "2020"=list(nyc=8804190, Bronx=1472654, Brooklyn=2736074, Manhattan=1694251, Queens=2405464, `Staten Island`=495747),
  "2021"=list(nyc=8467513, Bronx=1379946, Brooklyn=2590516, Manhattan=1596273, Queens=2278029, `Staten Island`=422749),
  "2022"=list(nyc=8335897, Bronx=1356476, Brooklyn=2561225, Manhattan=1597451, Queens=2278029, `Staten Island`=422716),
  "2023"=list(nyc=8258035, Bronx=1336705, Brooklyn=2530151, Manhattan=1596909, Queens=2245398, `Staten Island`=418872),
  "2024"=list(nyc=8258035, Bronx=1336705, Brooklyn=2530151, Manhattan=1596909, Queens=2245398, `Staten Island`=418872),
  "2025"=list(nyc=8258035, Bronx=1336705, Brooklyn=2530151, Manhattan=1596909, Queens=2245398, `Staten Island`=418872),
  "2026"=list(nyc=8258035, Bronx=1336705, Brooklyn=2530151, Manhattan=1596909, Queens=2245398, `Staten Island`=418872)
)

# ── Download precinct population data (2020 Census, via John Keefe/WNYC) ─────
cat("\nDownloading precinct population data...\n")
pop_resp <- tryCatch({
  GET("https://raw.githubusercontent.com/jkeefe/census-by-precincts/master/data/nyc/nyc_precinct_2020pop.csv",
      timeout(30))
}, error = function(e) NULL)

precinct_population <- list()
if (!is.null(pop_resp) && status_code(pop_resp) == 200) {
  pop_csv <- read.csv(text = content(pop_resp, "text", encoding = "UTF-8"))
  for (i in seq_len(nrow(pop_csv))) {
    pct_key <- as.character(pop_csv$precinct[i])
    precinct_population[[pct_key]] <- as.integer(pop_csv$P1_001N[i])
  }
  cat(sprintf("  Population data loaded for %d precincts\n", length(precinct_population)))
} else {
  cat("  WARNING: Could not download precinct population data\n")
}

# ── Build nested JSON structure ───────────────────────────────────────────────

cat("Building JSON...\n")
data_out <- list()
for (i in seq_len(nrow(agg))) {
  cr <- agg$ofns_desc[i]; lo <- agg$bucket[i];  bo <- agg$borough[i]
  yr <- agg$yr[i];        qt <- agg$quarter[i]; n  <- agg$n[i]
  if (is.null(data_out[[cr]]))             data_out[[cr]]             <- list()
  if (is.null(data_out[[cr]][[lo]]))       data_out[[cr]][[lo]]       <- list()
  if (is.null(data_out[[cr]][[lo]][[bo]])) data_out[[cr]][[lo]][[bo]] <- list()
  if (is.null(data_out[[cr]][[lo]][[bo]][[yr]])) data_out[[cr]][[lo]][[bo]][[yr]] <- list()
  data_out[[cr]][[lo]][[bo]][[yr]][[qt]] <- n
}

output <- list(
  meta = list(
    generated       = format(Sys.time(), "%Y-%m-%dT%H:%M:%S"),
    current_year    = current_year,
    current_quarter = paste0("Q", current_quarter),
    ytd_through     = sprintf("End of Q%d (%d months)", current_quarter, ytd_end_month)
  ),
  crime_types        = all_crime_types,
  crime_types_major  = major_in_list,
  crime_groups       = c("All crime", names(CRIME_GROUPS)),
  subway_ridership   = subway_ridership,
  population         = NYC_POP,
  data               = data_out,
  pct_data           = pct_data_out,
  precinct_population = precinct_population
)

out_path <- "crime_data.json"
write(toJSON(output, auto_unbox = TRUE), out_path)

# ── Download precinct boundaries GeoJSON ─────────────────────────────────────
cat("\nDownloading precinct boundaries...\n")
geo_resp <- tryCatch({
  GET("https://raw.githubusercontent.com/ResidentMario/geoplot-data/master/nyc-police-precincts.geojson",
      timeout(30))
}, error = function(e) NULL)

if (!is.null(geo_resp) && status_code(geo_resp) == 200) {
  writeLines(content(geo_resp, "text", encoding = "UTF-8"), "precincts.geojson")
  cat("  Saved precincts.geojson\n")
} else {
  cat("  WARNING: Could not download precincts.geojson — map will not work\n")
  cat("  Manually save from: https://raw.githubusercontent.com/ResidentMario/geoplot-data/master/nyc-police-precincts.geojson\n")
}

size_kb <- round(file.size(out_path) / 1024)
cat(sprintf("\nDone! Wrote %s (%d KB)\n", out_path, size_kb))
cat(sprintf("  %d individual crime types\n", length(all_crime_types)))
cat(sprintf("  YTD defined as Q1-Q%d of each year\n", current_quarter))
cat("\nRefresh your browser at http://127.0.0.1:4321\n")
