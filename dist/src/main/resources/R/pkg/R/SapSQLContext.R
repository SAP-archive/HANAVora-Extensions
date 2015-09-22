#' Initialize a new SapSQLContext.
#'
#' This function creates a SapSQLContext from an existing JavaSparkContext
#'
#' @param jsc The existing JavaSparkContext created with SparkR.init()
#' @export
#' @examples
#'\dontrun{
#' sc <- sparkR.init()
#' sqlContext <- sparkRVora.init(sc)
#'}
sparkRVora.init <- function(jsc = NULL) {
  if (exists(".sparkRVorasc", envir = SparkR:::.sparkREnv)) {
    return(get(".sparkRVorasc", envir = SparkR:::.sparkREnv))
  }

  # If jsc is NULL, create a Spark Context
  sc <- if (is.null(jsc)) {
    sparkR.init()
  } else {
    jsc
  }

  ssc <- SparkR:::callJMethod(sc, "sc")
  sapSQLCtx <- tryCatch({
    SparkR:::newJObject("org.apache.spark.sql.SapSQLContext", ssc)
  }, error = function(err) {
    stop("Spark SQL is not built with SapSQLContext support")
  })

  assign(".sparkRVorasc", sapSQLCtx, envir = SparkR:::.sparkREnv)
  sapSQLCtx
}
