library(dplyr)
library(tidyr)
library(magrittr)

rm(list=ls())
load('raw/works.RData')

# NULL blank movement names
works %<>% 
  mutate(movement = ifelse(movement=='', NA, movement))

# separate the work and movement IDs out of workID
works.sep <- works %>% 
  separate(workID, into=c('masterWorkID','mvmtID'), sep='\\*', remove=F, convert=T)


# are work titles consistent across ids?
# works.sep %>% 
#   group_by(work) %>% 
#   summarize(ntitles=n_distinct(workTitle)) %>% 
#   arrange(desc(ntitles))
# thankfully, yes


# tag works that have individual movement listings elsewhere
tmp <- works.sep %>%
  group_by(masterWorkID) %>% 
  summarize(has_mvmt = factor(if_else(sum(!is.na(mvmtID)) > 0, 'Y', 'N')))
works.sep %<>% inner_join(tmp)

works %<>% 
  inner_join(select(works.sep, workID, masterWorkID, has_mvmt))
rm(works.sep, tmp)


load('raw/program_work_performers.RData')
load('data/performers.RData')
load('data/composers_tmp.RData')

solos <- program_work_performers %<>% 
  filter(performerRole=='S')

works %>% 
  left_join(solos) %>% 
  left_join(performers) %>% 
  group_by(masterWorkID)