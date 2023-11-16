## ---------------------------
##
## Purpose of script: Trim Hauskoordinaten Daten downloaded from LDBV, Bayern
##
## Author: Bin Zhou
##
## Date Created: 2023-10-19
##
## Copyright (c) Bin Zhou, 2021
## Email: bin.zhou@med.uni-augsburg.de
##
## ---------------------------
##
## Notes: https://www.ldbv.bayern.de/produkte/kataster/hauskoordinaten.html
##   
## ---------------------------
library(data.table)
library(magrittr)
library(dplyr)


setwd("/Users/zhoubin/Projects/UA/P14.Geocoding/")

cols <- c('str','hnr','adz','postplz','postonm','kreis','regbez','land','ostwert','nordwert')


dat <- file.path('0.raw/moosach.csv') %>% 
  fread() %>% 
  select(all_of(cols)) %>% 
  mutate(kreis=gsub('Landkreis ','',kreis),
         regbez=gsub('Bezirksverwaltung ','',regbez))


file.path('0.raw/moosach_lite.csv') %>% 
fwrite(dat,.)



