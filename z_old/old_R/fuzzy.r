library(fuzzyjoin)
library(dplyr)


load('data/composers_tmp.RData')
era <- read.table('raw/composer_era.txt', sep='\t', header=T, quote='')

composers %>% 
  mutate(composerName = paste(fname, lname)) %>% 
  stringdist_inner_join(era, by=c(composerName='name'), max_dist=.5, method='soundex') %>% 
  select(composerName, name) %>% View
