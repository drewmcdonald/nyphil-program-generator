library(dplyr)
library(magrittr)

rm(list=ls())

setwd('~/Desktop/nyphil/')
load('raw/composers.RData')

# remove double spaces
composers %<>% 
  mutate(composerName = gsub(' +', ' ', composerName))
# extract brackets (can be multiple)
# split lname and other names
# swap in bracketed versions where necessary

# separate brackets
brackets <- composers$composerName %>% 
  stringr::str_extract_all('\\[[^,]+\\]', simplify=T) %>% 
  apply(
    MARGIN=2,
    FUN=function(x) {
      x[x==''] <- NA
      x <- gsub('(\\[|\\])', '', as.character(x))
      gsub('(^ | $)', '', x)
    }
  )
# positions remain safely aligned
composers[, c('brack1', 'brack2')] <- brackets; rm(brackets)

# clean up
composers %<>% 
  mutate(
    composerName = stringr::str_replace_all(composerName,'\\[[^,]+\\]', ''),
    composerName = gsub('(^ +| +$)', '', gsub('  +', ' ', gsub(' +,', ',', composerName)))
  )

# separate lname and fname
# this drops a lot of info in about 20 cases where there is more than one 
# composer, but fuck that. this matching is going to be imperfect no matter what
composers %<>%
  tidyr::separate(composerName, sep=', ', extra='drop', fill='right',
                  into=c('composerLastName', 'composerFirstName', 'composerOtherName'), remove=F)

# what's in the 'other' category besides double composer names?
composers %>% filter(!is.na(composerOtherName), !grepl(' (and|&)', composerName)) %>% View
# mostly honorifics and JR/SR type things


save(composers, file='data/composers_tmp.RData')
