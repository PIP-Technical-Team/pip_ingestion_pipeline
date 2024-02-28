#include <Rcpp.h>

using namespace Rcpp;

// [[Rcpp::export]]
NumericVector SelectCumGrowth(NumericVector gdpg,
                              NumericVector pceg,
                              Rcpp::StringVector inc_group) {
  
  int n = gdpg.size();
  NumericVector out(n);
  
  // Initialize first element
  out[0] = 1;
  
  // Loop through remaining elements
  for (int i = 1; i < n; ++i) {
    if (Rcpp::NumericVector::is_na(pceg[i])) {
      out[i] = gdpg[i] * out[i - 1];
    } else if (Rcpp::NumericVector::is_na(gdpg[i])) {
      out[i] = pceg[i] * out[i - 1];
    } else if (inc_group[i] == "LIC" || inc_group[i] == "LMIC") {
      out[i] = gdpg[i] * out[i - 1];
    } else {
      out[i] = pceg[i] * out[i - 1];
    }
  }
  
  return out;
}

