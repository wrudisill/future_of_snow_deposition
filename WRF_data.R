##Script to create data frame from AWS from WRF outputs for hoar frost dataset
##Oct 2022
#Author: Arielle Koshkin


library(tidyverse)
library(aws.s3) #access aws server
library(tidync) ##read netcdf
library(lubridate)

##check to see if bucket exists
bucket_exists(
  bucket = "s3://wrf-cmip6-noversioning", 
  region = "us-west-2"
)
Sys.setenv("AWS_DEFAULT_REGION" = 'us-west-2')

##get data from bucket in list. Updated to ERA.
WRF.list <- get_bucket(
  bucket = "s3://wrf-cmip6-noversioning", #general bucket name
  prefix="downscaled_products/reanalysis/era5/hourly/2014/d02", ##folder in bucket
  # region = "us-west-2",
  max = 9000
) 

# Identify dates in WRF.list above. 
info <- data.frame(filename = map_chr(WRF.list, ~.$Key)) %>% 
  mutate(date_time = basename(filename)) |>
  mutate(date_time = str_remove(date_time, "auxhist_d01_")) %>% 
  mutate(date_time = ymd_hms(date_time)) %>% 
  mutate(date = date(date_time))

dates <- unique(info$date)

system.time(
  for (i in 1:length(dates)){
    date_info <- info %>% filter(date == dates[i])
    
    date_dat <- map_dfr(date_info$filename, function(x){
      #open netcdf and select for varrible we care about (TSK, Q2, T2)
      temp.d<-aws.s3::s3read_using(hyper_tibble, object = paste0("s3://wrf-cmip6-noversioning/", 
                                                                 x)) %>%
        select("west_east","south_north", "SNOW", "TSK", "T2", "Q2", "PSFC" )%>%
        mutate(time_hour = str_sub(WRF.list[i]$Contents$Key, -8, -7)) %>%
        mutate(Date = str_sub(WRF.list[i]$Contents$Key, -19, -10)) %>%
        mutate(month = str_sub(WRF.list[i]$Contents$Key, -14, -13))
      
      # ##add temp.d to dataframe
      #   hoar_frost.df<-rbind(temp.d, hoar_frost.df) # 91800 cells, one hour. 
      temp.d <- temp.d %>%
        # mutate(Date = as_date(Date))%>%
        mutate(T2_C = T2 - 273.1)%>%
        mutate(TSK_C = TSK - 273.1)%>%
        #filter(T2_C > -50 & T2_C < 102) %>% #The Goff Gratch equation [1] for the vapor pressure over ice covers a region of -50°C to 102°C [Gibbins 1990]
        mutate(esat_T2 =  10^ (-9.09718 * (273.16/T2 - 1)##in hpa
                               - 3.56654 *log10(273.16/ T2)
                               + 0.876793* (1 - T2/ 273.16)
                               + log10(6.1071)))%>%
        mutate(esat_TSK =  10^ (-9.09718 * (273.16/TSK - 1) ##in hpa
                                - 3.56654 *log10(273.16/ TSK)
                                + 0.876793* (1 - TSK/ 273.16)
                                + log10(6.1071)))%>%
        mutate(Snow_present = ifelse(SNOW > 1, T, F))%>% # SWE in mm
        mutate(e_T2 = Q2*PSFC/0.622/100)%>% #hpa (converted pa to hpa to match esat)
        mutate(deposition = ifelse(e_T2 > esat_TSK, T, F))%>%
        select(west_east, south_north, Snow_present, deposition, e_T2, esat_TSK)
      return(temp.d)
    }) # end function applied over hours within a date. 
    
    date_dat2 <- date_dat %>%
      group_by(west_east, south_north) %>% 
      summarise(snow = sum(Snow_present),
                deposition = sum(deposition),
                max_gradient = max(e_T2 - esat_TSK)) %>% # direction is air into surface. 
      ungroup() 
    
    write_csv(date_dat2, paste0("data/ERA/daily_", date_info$date[1], ".csv"))
      
    return(date_dat2)
  } # end looping over dates. 
) # currently takes 46 seconds per day. Would be ~29 hours if parallelized over 8 cores for a 50 year run.
# 3 MB/day --> 50 GB for a 50 year run (roughly 1 GB/year).

# AM stopped here January 26. 

## Next: calculate some annual stats. 

# This would need to be reworked to match new data structure. 
dep.df <- hoar_frost.df%>%
   filter(Snow_present ==T)%>%
   filter(deposition == T)%>%
   group_by(west_east, south_north, Date)%>%
   tally()%>% 
   ungroup() %>% 
   filter(n > 0) %>% 
   group_by(west_east, south_north) %>% 
   tally()

snow_p <- hoar_frost.df%>%
  group_by(west_east, south_north)%>%
  summarise(n=sum(Snow_present))%>%
  mutate(snow_days=n/8760)

ggplot() +
  geom_raster(data = snow_p, aes(x=west_east, y=south_north, fill = snow_days))+
  theme_cowplot(12)+
  coord_equal()



library(USAboundaries)

mystates <- us_states(states = c("Washington", "Oregon", "California",
              "Nevada", "Idaho", "Wyoming", "Arizona", "New Mexico", "Colorado", "Utah", "Montana"))
  #st_transform(9802)
  
ggplot() + geom_sf(data = mystates)+
  geom_raster(data = dep.df, aes(x=west_east, y=south_north, fill = n))+
  theme_cowplot(12)

ggplot(year_dep)+
  geom_line(aes(x=Date, y= n))+
  theme_cowplot()

         


