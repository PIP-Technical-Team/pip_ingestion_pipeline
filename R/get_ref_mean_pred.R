get_ref_mean_pred <- \(old, new) {
  

  dr <- joyn::joyn(
    x = old,
    y = new,
    by = c(
      "country_code",
      "reporting_level",
      "welfare_type",
      "survey_year"),
    match_type = "1:m",
    reportvar = FALSE,
    keep = "right"
  ) |> 
  # format variables
  frename(ref_mean = predicted_mean_ppp)
  
  dr
  
}