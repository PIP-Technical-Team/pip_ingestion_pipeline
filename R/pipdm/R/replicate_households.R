library(ggplot2)
library(data.table)
library(collapse)

# Step 1: Identify Outliers
find_outliers <- \(DT, weight = "weight", threshold = 2.5) {
  mean_w <- fmean(DT[[weight]])
  sd_w   <- fsd(DT[[weight]])
  
  DT[, is_outlier := .SD[[1]] > (mean_w + threshold * sd_w), .SDcols = weight]
  DT
}

# Step 2: Calculate Replications and Partitioning for Outliers
calculate_replications <- \(DT, weight = "weight") {
  mean_w <- fmean(DT[[weight]])
  
  # Get houdehold ID
  DT[, hhid := .I]
  # Get replication count
  DT[, rep_count := as.integer(pmax(round(.SD[[1]]  / mean_w), 2)), 
     .SDcols = weight
  ][is_outlier == FALSE,
    rep_count := 1]
  
  Y <- DT[rep(1:.N,rep_count)]
  Y
}

split_weights <- \(x, rep) {
  base <- floor(x / rep)
  rem <- x[1] - fsum(base) 
  base[length(base)] <- base[1] + rem
  base
}

add_new_weights <- \(DT, weight = "weight") {
  X <- DT[is_outlier == TRUE]
  Y <- DT[is_outlier == FALSE]
  X[, x := split_weights(.SD[[1]], rep_count), 
     by = hhid, 
     .SDcols = weight]
  setnames(X, c(weight, "x"), c("x", weight)) # reverse names
  X[, x := NULL]
  
  rowbind(Y,X, fill = TRUE)
}

clean_new_weights <- \(DT, ori_names) {
  DT <- DT[, ..ori_names]
}

lorenz_table <- \(x, nq = 100) {
  x |>
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
  
}
# Wrapper Function
replicate_households <- function(DT, weight = "weight", threshold = 2) {
  R <- copy(DT)  # work on a copy to avoid modifying original DT
  ori_names <- R |> 
    names() |> 
    copy()
  R <- find_outliers(R, weight, threshold)
  R <- calculate_replications(R, weight)
  R <- add_new_weights(R, weight)
  R <- clean_new_weights(R, ori_names)
  lt <- lorenz_table(R) # this is very inefficient, but that's whay I have for now
  
  welfare_share_bad <- any(diff(lt$welfare_share) < 0) 
  setattr(R, "welfare_share_OK", !welfare_share_bad)
  setattr(R, "threshold", threshold)
  
  if (welfare_share_bad && threshold > 0) {
    threshold <- max(threshold - .5, 0)
    R <- replicate_households(DT, weight, threshold)
  }
  R
}

# Example Usage:
DT <- data.table(
  A = letters[1:7],
  B = 101:107,
  weight = c(10, 31, 12, 9, 8, 90, 124)
)

DT_new <- replicate_households(DT, weight = "weight", threshold = 1)
DT_new[]


civ <- pipload::pip_load_cache("CIV", 2002)
ury <- pipload::pip_load_cache("PRY", 2018)


ury  |>  
  ggplot(aes(x = weight)) +
    geom_histogram(bins = 100) +
    geom_vline(aes(xintercept=fsd(weight)*2.5+fmean(weight)),
               color="blue", linetype="dashed", linewidth=1) +
    geom_density(alpha=.2, fill="#FF6666") 


civ2 <- replicate_households(civ, threshold = 1)
nrow(civ2)



civp1 <- lorenz_table(civ) 
civp2 <- lorenz_table(civ2)

perr <- which(diff(civp1$welfare_share) < 0)
perr2 <- which(diff(civp2$welfare_share) < 0)

civp1[perr]
civp1[perr2]
civp2[c(perr2, perr2+1) |> sort()]




dim(civ2)
