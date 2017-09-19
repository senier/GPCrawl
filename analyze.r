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

tlds <- dbGetQuery(con, "SELECT count(*) AS count, split_part(details_appdetails_packagename,'.',1) AS name FROM apps GROUP BY name ORDER BY count desc LIMIT 8")
pie(tlds$count, labels = tlds$name, main = 'Apps by package TLD', col=rainbow(length(tlds$count)))

dl <- dbGetQuery(con, "SELECT details_appdetails_numdownloads as downloads FROM apps WHERE details_appdetails_numdownloads > 0")
hist = hist(dl$downloads, breaks = 50, plot = FALSE)
plot(hist, col = "green", main = "Downloads", xlab = 'downloads', ylim = c(0.00001,200000))

data <- dbGetQuery(con, "SELECT search, creator, title, aggregaterating_starrating AS rating, (details_appdetails_file_size/1024/1024) as size, details_appdetails_numdownloads as downloads FROM apps")

hist = hist(data$size, col = "green", breaks = 50, main = "File Size", xlab = 'Size in MB')

hist = hist(data$rating, col = "green", breaks = 40, main = "Rating")

da <- sort(table(data$creator), decreasing = TRUE)
par(las=2, cex = 0.6, mar = c(20,3,5,3))
plot(x = da[1:50], type= "p", main="# creators (top 50)", xlab = '', ylab = "# apps")
