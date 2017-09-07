#!/usr/bin/env Rscript

library(base64enc)
library(RPostgreSQL)

# Read db password from file
password <- scan("~/.pgpass", what="")

# PostgreSQL driver
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "google_play_test", host = "localhost", port = 5432, user = "alex", password = password)
dbExistsTable(con, "apps")

# Disable scientific numbers
options(scipen=999)

data <- dbGetQuery(con, "SELECT search, creator, title, aggregaterating_starrating AS rating FROM apps")

hist = hist(data$rating, col = "green", breaks = 40, main = "Rating")
plot(table(data$search), type= "p", main="# search results")

da <- sort(table(data$creator), decreasing = TRUE)
par(las=2, cex = 0.6, mar = c(20,3,5,3))
plot(x = da[1:50], type= "p", main="# creators ", xlab = '', ylab = "# apps", ylim=c(0,100))
