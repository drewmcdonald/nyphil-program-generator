library(jsonlite)
library(dplyr)
library(magrittr)
library(lubridate)
library(progress)

rm(list=ls())
setwd('~/Desktop/nyphil')

# read in file
data <- read_json('complete.json')
data <- data$programs

# lookup keys
program_keys <- c('id','programID','orchestra','season')
work_keys <- c('ID','programID','composerName',
               'workTitle','movement','conductorName')

# Receptacles
programs <- list()  
concerts <- list()
program_works <- list()
program_work_performers <- list()

# progress bar
pb <- progress_bar$new(total = length(data))

# loop throughdata
for (i in 1:length(data)) {
  
  d <- data[[i]]
  
  pb$tick()  # progress bar

  pid <- d$programID
  programs <- append(programs, list(d[program_keys]))
  
  # program concerts
  for(c in d$concerts) {
    c$programID <- pid
    concerts <- append(concerts, list(c))
  }
  # program works
  for(w in d$works) {
    wid <- w$ID
    w$programID <- pid
    # some works have a multipart title or movement (including an 'em' key?)
    # maybe a specific program notation, but I'm just concatting for now
    w$movement <- paste(w$movement, collapse=', ')
    w$workTitle <- paste(w$workTitle, collapse=', ')
    # fill out missing keys as NA (makes bind_rows work later)
    for(wk in work_keys)
      if(is.null(w[[wk]])) w[[wk]] <- NA
    program_works <- append(program_works, list(w[work_keys]))
    # program-work performers
    for(s in w$soloists) {
      s$programID <- pid
      s$workID <- wid
      program_work_performers <- append(program_work_performers, list(s))
    }
  }
}
# clean up R's bullshit
rm(s, w, wk, d, c, data, i, pid, wid, program_keys, work_keys, pb)

# print multipart work titles/movements (before paste fix included above
# works <- do.call(rbind, works) %>% as.data.frame()
# works[which(sapply(works$workTitle, length)> 1), c('programID', 'ID')]
# works[which(sapply(works$movement, length)> 1), c('programID', 'ID')]

# dataframes, names, and type conversions
programs %<>% 
  bind_rows() %>% 
  mutate(
    programID = as.integer(programID),
    season = as.integer(substr(season, 1, 4))  # start year only
  )

concerts %<>% 
  bind_rows() %>% 
  mutate(
    concertID = row_number(),
    datetime = parse_date_time(
      paste(substr(Date,1,10), Time), 
      'Ymd HM', tz='EST', truncated=2),
    programID = as.integer(programID)
  ) %>% 
  select(-Time, -Date)

program_work_performers %<>% 
  bind_rows() %>% 
  mutate(
    programID = as.integer(programID)
  )

program_works %<>% 
  bind_rows() %>% 
  rename(workID=`ID`) %>% 
  mutate(
    programID = as.integer(programID)
  ) %>% 
  group_by(programID) %>% 
  mutate(programOrder = row_number()) %>% 
  ungroup()

# split off venues from concerts
venues <- concerts %>% 
  distinct(Location, Venue) %>% 
  mutate(venueID = row_number())
concerts %<>% 
  inner_join(venues, by=c('Location','Venue')) %>% 
  select(concertID, programID, venueID, eventType, datetime)

# split off works from program_works
works <- program_works %>% 
  distinct(workID, composerName, workTitle, movement)
program_works %<>% 
  select(-composerName, -workTitle, -movement)

# split off composers from works
composers <- works %>% 
  distinct(composerName) %>% 
  mutate(composerID = row_number())
works %<>% 
  inner_join(composers, by=c('composerName')) %>% 
  select(composerID, workID, workTitle, movement)

# split off performers from program_work_performers
# rename soloistRoles to performerRole
performers <- program_work_performers %>% 
  distinct(soloistName, soloistInstrument) %>% 
  mutate(performerID = row_number())
program_work_performers %<>% 
  inner_join(performers, by=c('soloistName', 'soloistInstrument')) %>% 
  select(programID, workID, performerID, performerRole=soloistRoles)
performers %<>% 
  rename(name=soloistName, instrument=soloistInstrument)

# split off conductors from program_works
conductors <- program_works %>% 
  distinct(conductorName) %>% 
  mutate(conductorID = row_number())
program_works %<>% 
  inner_join(conductors, by='conductorName') %>% 
  select(programID, workID, conductorID, programOrder)

# check tables and save
for(t in ls()) {
  cat('\n', t, '\n')
  glimpse(eval(rlang::sym(t)))
  save(list=t, file=paste0('raw/', t, '.RData'))
}; rm(t)
