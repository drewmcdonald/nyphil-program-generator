library(dplyr)
library(magrittr)

rm(list=ls())

setwd('~/Desktop/nyphil/')
load('raw/concerts.RData')

# # look at programs for various eventTypes
# load('raw/programs.RData')
# load('raw/program_works.RData')
# load('raw/works.RData')
# load('raw/composers.RData')
# 
# concerts %>% 
#   filter(eventType=="Young People's Concert") %>% 
#   distinct(programID, .keep_all=T) %>% 
#   # sample_n(5) %>% 
#   inner_join(programs) %>% 
#   inner_join(program_works) %>% 
#   left_join(works) %>% 
#   inner_join(composers) %>% 
#   select(
#     concertID, programID, workID, programOrder, composerName, workTitle, movement
#   ) %>% View

# add a broader category of concert
concerts %<>% 
  mutate(
    eventType = factor(eventType),
    eventTypeBroad = recode_factor(eventType,
                                   "Artist in Residence recital"="Recital",
                                   "Borough Concerts"="Symphonic",
                                   "Carnegie Pop Concert"="Symphonic",
                                   "Chamber"="Chamber",
                                   "Chamber Concert (Private)"="Chamber",
                                   "Contact!"="Other",
                                   "Festival"="Symphonic",
                                   "Festival - Chamber"="Chamber",
                                   "Hear & Now"="Modern",
                                   "Holiday Brass"="Other",
                                   "Horizons"="Modern",
                                   "Informal Evening"="Symphonic",
                                   "Inside the Music"="Symphonic",
                                   "Insight Series"="Other",
                                   "Lecture"="Other",
                                   "Leinsdorf Lecture"="Other",
                                   "Master Class"="Other",
                                   "New Year's Eve"="Symphonic",
                                   "Non-Subscription"="Symphonic",
                                   "None"="Symphonic",
                                   "NY Phil Biennial"="Other",
                                   "Off the Grid"="Chamber",
                                   "Parks"="Symphonic",
                                   "Parks - Free Indoor Concert"="Symphonic",
                                   "Pension Fund Benefit Concert"="Symphonic",
                                   "Pre-Concert Recital"="Recital",
                                   "Private Concert"="Chamber",
                                   "Promenade"="Symphonic",
                                   "Prospective Encounters"="Modern",
                                   "Reading Rehearsal"="Other",
                                   "Recording Session Only"="Other",
                                   "Residency"="Other",
                                   "Residency - Chamber"="Chamber",
                                   "Rug Concerts"="Symphonic",
                                   "Runout"="Symphonic",
                                   "Rush Hour"="Symphonic",
                                   "Saturday Matinee"="Symphonic",
                                   "Soloist Recital"="Recital",
                                   "Special"="Symphonic",
                                   "Sponsor Chamber Music"="Chamber",
                                   "St. John the Divine"="Symphonic",
                                   "Stadium Concert"="Symphonic",
                                   "Strike Concerts"="Symphonic",
                                   "Student Concert"="Symphonic",
                                   "Subscription Season"="Symphonic",
                                   "Summer Broadcast Concert"="Symphonic",
                                   "Summertime Classics"="Symphonic",
                                   "Tour"="Symphonic",
                                   "Tour - Chamber"="Chamber",
                                   "Tour - Concert for Patrons"="Chamber",
                                   "Tour - Young People's Concert"="Young People's",
                                   "Tour - Young People's Concerts for Schools"="Young People's",
                                   "Tour Very Young People's Concert"="Young People's",
                                   "Very Young People's Concert"="Young People's",
                                   "World Trade Center - Chamber"="Chamber",
                                   "World's Fair"="Symphonic",
                                   "Young People's Concert"="Young People's",
                                   "Young People's Concerts for Schools"="Young People's"
    ),
    modelable = recode_factor(eventType,
                              "Artist in Residence recital"="N",
                              "Borough Concerts"="Y",
                              "Carnegie Pop Concert"="Y",
                              "Chamber"="N",
                              "Chamber Concert (Private)"="N",
                              "Contact!"="N",
                              "Festival"="Y",
                              "Festival - Chamber"="N",
                              "Hear & Now"="N",
                              "Holiday Brass"="N",
                              "Horizons"="N",
                              "Informal Evening"="Y",
                              "Inside the Music"="N",
                              "Insight Series"="N",
                              "Lecture"="N",
                              "Leinsdorf Lecture"="N",
                              "Master Class"="N",
                              "New Year's Eve"="Y",
                              "Non-Subscription"="N",
                              "None"="Y",
                              "NY Phil Biennial"="N",
                              "Off the Grid"="N",
                              "Parks"="Y",
                              "Parks - Free Indoor Concert"="Y",
                              "Pension Fund Benefit Concert"="Y",
                              "Pre-Concert Recital"="N",
                              "Private Concert"="N",
                              "Promenade"="Y",
                              "Prospective Encounters"="N",
                              "Reading Rehearsal"="N",
                              "Recording Session Only"="N",
                              "Residency"="N",
                              "Residency - Chamber"="N",
                              "Rug Concerts"="Y",
                              "Runout"="Y",
                              "Rush Hour"="Y",
                              "Saturday Matinee"="Y",
                              "Soloist Recital"="N",
                              "Special"="Y",
                              "Sponsor Chamber Music"="N",
                              "St. John the Divine"="Y",
                              "Stadium Concert"="Y",
                              "Strike Concerts"="Y",
                              "Student Concert"="Y",
                              "Subscription Season"="Y",
                              "Summer Broadcast Concert"="Y",
                              "Summertime Classics"="N",
                              "Tour"="Y",
                              "Tour - Chamber"="N",
                              "Tour - Concert for Patrons"="N",
                              "Tour - Young People's Concert"="N",
                              "Tour - Young People's Concerts for Schools"="N",
                              "Tour Very Young People's Concert"="N",
                              "Very Young People's Concert"="N",
                              "World Trade Center - Chamber"="N",
                              "World's Fair"="Y",
                              "Young People's Concert"="N",
                              "Young People's Concerts for Schools"="N"
    )
  )

save(concerts, file='data/concerts.RData')
