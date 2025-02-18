library(data.table)
library(collapse)

# Step 1: Identify Outliers
find_outliers <- function(DT, weight = "weight", threshold = 2.5) {
  mean_w <- fmean(DT[[weight]])
  sd_w   <- fsd(DT[[weight]])
  
  DT[, is_outlier := .SD[[1]] > (mean_w + threshold * sd_w), 
     .SDcols = weight]
  return(DT)
}

# Step 2: Calculate Replications and Partitioning for each outlier row
calculate_replications <- function(DT, weight = "weight") {
  mean_w <- fmean(DT[[weight]])
  
  # For each outlier row, calculate replication_count and a list column of split_values.
  DT[is_outlier == TRUE, `:=`(
    replication_count = {
      x <- .SD[[1]]  # using .SD[[1]] to extract the value for the weight column
      as.integer(pmax(round(x / mean_w), 2))
    },
    split_values = {
      x <- .SD[[1]]
      rep_count <- as.integer(pmax(round(x / mean_w), 2))
      base <- floor(x / rep_count)
      rem <- x - rep_count * base
      # Partition into (rep_count - rem) copies of base and rem copies of (base+1)
      list(c(rep(base, rep_count - rem), rep(base + 1, rem)))
    }
  ), by = .I, .SDcols = weight]
  
  return(DT)
}

# Step 3: Expand Outlier Rows and Create new_weight Column
expand_outliers <- function(DT, weight = "weight", ori_names) {
  # For outliers, replicate each row replication_count times and replace weight with partition values.
  DT_expanded <- DT[is_outlier == TRUE, {
    count <- replication_count[1]  # each group has one row
    new_weight <- unlist(split_values)  # partitioned weight values
    replicated_row <- .SD[rep(1L, count)]
    replicated_row[, new_weight := new_weight]
    replicated_row
  }, by = .I]
  setnames(DT_expanded, old = "new_weight", weight)
  
  # Remove helper columns from expanded outliers.
  DT_expanded <- DT_expanded[, ..ori_names]
  
  # For non-outlier rows, simply copy the original weight to new_weight.
  DT_non_outliers <- DT[is_outlier == FALSE
  ][, ..ori_names]
  
  # Combine non-outlier rows with the expanded outliers.
  DT_final <- rbind(DT_non_outliers, DT_expanded, fill = TRUE)
  
  return(DT_final)
}

# Wrapper Function
replicate_households <- function(DT, weight = "weight", threshold = 2.5) {
  DT <- copy(DT)  # work on a copy to avoid modifying original DT
  ori_names <- DT |> 
    names() |> 
    copy()
  DT <- find_outliers(DT, weight, threshold)
  DT <- calculate_replications(DT, weight)
  DT <- expand_outliers(DT, weight, ori_names)
  return(DT)
}

# Example Usage:
DT <- data.table(
  A = letters[1:7],
  B = 101:107,
  weight = c(10, 31, 12, 9, 8, 90, 124)
)

DT_new <- replicate_households(DT, weight = "weight", threshold = 1)
DT_new[]


library(ggplot2)

civ <- pipload::pip_load_cache("CIV", 2002)
ury <- pipload::pip_load_cache("PRY", 2018)


ury  |>  
  ggplot(aes(x = weight)) +
    geom_histogram(bins = 100) +
    geom_vline(aes(xintercept=fsd(weight)*2.5+fmean(weight)),
               color="blue", linetype="dashed", linewidth=1) +
    geom_density(alpha=.2, fill="#FF6666") 

setorder(civ, welfare)
civp <- wbpip::md_compute_quantiles(welfare = civ$welfare, 
                                    weight = civ$weight, 
                                    n_quantile = 100000)
length(civp)
any(diff(civp) < 0)



nq <- 100


dt <- civ |>
  ## totals -----------
fgroup_by(c("welfare_type")) |>
  fmutate(tot_pop = fsum(weight),
          tot_wlf = fsum(welfare_ppp*weight)) |>
  fungroup() |>
  setorder(welfare_type, welfare_ppp) |>
  fgroup_by(c("welfare_type", "reporting_level")) |>
  fmutate(bin = wbpip:::md_compute_bins(welfare = welfare_ppp,
                         weight = weight,
                         nbins = nq)) |>
  fungroup() |> 
  ## shares at the observation level ---------
ftransform(pop_share = weight/tot_pop,
           welfare_share = (welfare_ppp*weight)/tot_wlf) |>
  ## aggregate
  fgroup_by(reporting_level, welfare_type, bin) |>
  fsummarise(avg_welfare    = fmean(welfare_ppp, w = weight),
             pop_share      = fsum(pop_share),
             welfare_share  = fsum(welfare_share),
             quantile       = fmax(welfare_ppp),
             pop            = fsum(weight)) |>
  fungroup()

perr <- which(diff(dt$welfare_share) < 0)
if (length(perr)) {
  dt[perr]
}

dim(civ)

civ2 <- replicate_households(civ)
