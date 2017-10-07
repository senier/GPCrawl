#!/usr/bin/env Rscript

# To use this script libpq-dev, R and the postgres interface for R must be installed:
#
# # apt install r-base-core libpq-dev
#
# In R do:
# install.packages("RPostgreSQL")
#
# RPostgreSQL does not seem to be able to conenct to a database with the default user
# and no password. Set a user password in Postgres and add it to the ~/.pgpass file:
#
# posggres=# ALTER USER <someuser> WITH PASSWORD '<somepassword>';
# ALTER ROLE
# $ echo '<somepassword>' > ~/.pgpass
# $ chmod 600 ~/.pgpass

library(RPostgreSQL)

# Read db password from file
password <- scan("~/.pgpass", what="")

# PostgreSQL driver
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "google_play_test", host = "localhost", port = 5432, user = "senier", password = password)
dbExistsTable(con, "apps")

# Disable scientific numbers
options(scipen=999)

paid <- dbGetQuery(con, "SELECT COUNT(offer_checkoutflowrequired) count, offer_checkoutflowrequired AS paied FROM apps GROUP BY offer_checkoutflowrequired")
pie(paid$count, labels = c('free', 'paid'), main = 'Paid vs. free apps')

price_euro <- dbGetQuery(con, "select apps.offer_micros/1000000.0 as price from apps where offer_checkoutflowrequired = true and apps.offer_currencycode = 'EUR'")
hist = hist(price_euro$price, breaks = 1500, col = "blue", main = "App price for paid apps", xlab = 'Price [EUR]', xlim = c(0,25))

tlds <- dbGetQuery(con, "SELECT count(*) AS count, split_part(details_appdetails_packagename,'.',1) AS name FROM apps GROUP BY name ORDER BY count desc LIMIT 8")
pie(tlds$count, labels = tlds$name, main = 'Apps by package TLD', col=rainbow(length(tlds$count)))

dl <- dbGetQuery(con, "SELECT details_appdetails_numdownloads as downloads, aggregaterating_ratingscount as ratings, aggregaterating_commentcount as comments FROM apps WHERE details_appdetails_numdownloads > 0")

hist = hist(dl$downloads, breaks = 50, plot = FALSE)
plot(hist, col = "green", main = "Downloads", xlab = 'downloads', ylim = c(0.00001,200000))

hist(dl$ratings, col = "red", main = "Ratings", xlab = 'ratings')
hist(dl$comments, col = "blue", main = "Comments", xlab = 'comments')

data <- dbGetQuery(con, "SELECT search, creator, title, aggregaterating_starrating AS rating, (details_appdetails_file_size/1024/1024) as size, details_appdetails_numdownloads as downloads FROM apps")

hist = hist(data$size, col = "green", breaks = 50, main = "File Size", xlab = 'Size in MB')

hist = hist(data$rating, col = "green", breaks = 40, main = "Rating")

da <- sort(table(data$creator), decreasing = TRUE)
par(las=2, cex = 0.6, mar = c(20,3,5,3))
plot(x = da[1:50], type= "p", main="# creators (top 50)", xlab = '', ylab = "# apps")
