##############################################################
##
## dbplot for ploting from spark.
##
## May 14, 2018
## janae.nicholson@okstate.edu
##
###############################################################

#first load the packages you need
library(sparklyr)
library(dplyr, warn.conflicts = FALSE)
library(dbplot)
library(ggplot2)
library(nycflights13)

#Connect to Spark
sc <- spark_connect(master = "local")

#copy some example data into spark
spark_flights <- sdf_copy_to(sc, nycflights13::flights, "flights")
#remove year since all are 2013
spark_flights <- select(spark_flights, -year)
sdf_dim(spark_flights)

spark_flights %>% 
  dbplot_histogram(sched_dep_time)

spark_flights %>% 
  dbplot_histogram(sched_dep_time, binwidth = 200)

spark_flights %>% 
  dbplot_histogram(sched_dep_time, binwidth = 300) +
  labs(title = "Flights - Scheduled Departure Time") +
  theme_bw()

spark_flights %>%
  filter(!is.na(arr_delay)) %>%
  dbplot_raster(arr_delay, dep_delay) 

spark_flights %>%
  filter(!is.na(arr_delay)) %>%
  dbplot_raster(arr_delay, dep_delay, mean(distance, na.rm = TRUE)) 

spark_flights %>%
  filter(!is.na(arr_delay)) %>%
  dbplot_raster(arr_delay, dep_delay, mean(distance, na.rm = TRUE), resolution = 500)

spark_flights %>%
  dbplot_bar(origin)

spark_flights %>%
  dbplot_bar(origin, mean(dep_delay))  +
  ylab("Mean Departure Time")

spark_flights %>%
  dbplot_line(month)

spark_flights %>%
  dbplot_line(month, mean(dep_delay))

spark_flights %>%
  dbplot_boxplot(origin, dep_delay)

spark_flights %>%
  db_compute_bins(arr_delay) 

spark_flights %>%
  filter(arr_delay < 100 , arr_delay > -50) %>%
  db_compute_bins(arr_delay) %>%
  ggplot() +
  geom_col(aes(arr_delay, count, fill = count))

#can use the functions to return a summarized data table
spark_flights %>%
  group_by(x = !! db_bin(dep_time)) %>%
  tally()

#create a histogram using the bin function
spark_flights %>%
  filter(!is.na(dep_time)) %>%
  group_by(x = !! db_bin(dep_time)) %>%
  tally()%>%
  collect %>%
  ggplot() +
  geom_col(aes(x, n), fill = "blue")

#can also replace tally with a function
spark_flights %>%
  filter(!is.na(dep_time)) %>%
  group_by(bin_dep_time = !! db_bin(dep_time)) %>%
  summarise(mean_arr_delay = mean(arr_delay, na.rm = TRUE))%>%
  collect %>%
  ggplot() +
  geom_col(aes(bin_dep_time, mean_arr_delay), fill = "#c6203e") 

#now what if this is a new data set that I want to profile?
#for the numeric variables loop through and create a raster with arr_delay
numeric_spark_flights <- select_if(spark_flights, is.numeric)
numeric_names <- dplyr::tbl_vars(numeric_spark_flights)
numeric_names <- numeric_names[numeric_names != "arr_delay"]
#create a local list to hold ggplot2 objects
temp_numeric_plot <- vector(mode = "list", length = length(numeric_names))

#loop through and create ggplot2 objects, note can't plot inside loop since this is in spark
for(num_plot in 1:length(numeric_names)){
  temp_numeric_plot[[num_plot]] <- numeric_spark_flights %>% 
    dbplot_raster(!!rlang::sym(numeric_names[num_plot]), arr_delay)
}

#now plot the ggplot2 objects,
#this extra step is needed since we did our computation in spark.
pdf("output/Numeric_Raster_plots.pdf", onefile = TRUE)
for(tni in 1:length(temp_numeric_plot)){
  print(temp_numeric_plot[[tni]])
}
dev.off() #close connection to pdf

#disconnect from Spark
spark_disconnect(sc)
