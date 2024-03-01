get_ref_mean_pred <- \(old, new) {
  

  dr <- joyn::joyn(
    x = old,
    y = new,
    by = c(
      "country_code",
      "reporting_level",
      "welfare_type",
      "reporting_year",
      "survey_year"
    ),
    match_type = "1:1",
    keep = "left",
    reportvar = FALSE
  )
  
  
  
  # format variables
  dr <- dr |> 
    frename(predicted_mean_ppp = old_predicted_mean_ppp, 
            ref_mean           = predicted_mean_ppp)
  
  dr
  
}