# Notes


## TODOs

- Clean up work title and movement name (spaces, punctuation)
- Feature detectors for
    + Op #
    + Piece type
    + key
    + arranger/is arrangement
    + final parenthetical notes
- set up musicbrainz composer scan and then work scan

## Data structure

Basic Concert-level Lookup Tables
- Orchestra
- Event Type
- Venue

Composer
- unique by name
- has many *Works*

Performer: conductors, soloists, and accompanists
- unique by name and instrument

Movement: subunit of a composition
- unique by work and movement number
- belongs to one *Work*

Work: primary unit of a composition
- unique by *Composer* and title
- belongs to one *Composer*
- has zero, one, or many *Movements*
- has one or two *Selections*

Selection: a hybrid unit of a composition
- unique by *Work* and an is_full_work flag
- belongs to one *Work*
- one entry for a whole *Work*
- one entry for a generic set of movements from a work
    + performing movements 1 and 4 from work X refers to the same selection record as performing movements 2 and 3

Concert: a performance of a set of *Selections*
- unique by datetime and \_\_\_(?) 


### Relationships

ConcertSelection: 
- concert ID, selection ID, and performance order

ConcertSelectionMovement: which movements comprised a concert's *Selection*
- ConcertSelection ID, movement ID, and performance order
- no entry for when ConcertSelection is an entire work

ConcertSelectionPerformer
- ConcertSelection ID, Performer ID, and role
- *not* going to track the soloist per actual movement


### Post-Processing

- Delete Intermissions from concert_selection where intermission is last (because it splits movements of a complete work)


## Model Structure

- Data: 
    + matrix of selections by program and concert, with 1+ categorical features

- Training:
    + options for case weighting or non-linear probability adjustments
    + one sub-model per categorical feature
    + split feature, interpolate intermission and begin/end flags, collapse

- Prediction:
    + adjust weights by feature
    + sum weighted probabilities as predicted by sub-models
    + sample from the weighted probabilities

- Sub-models:
    + Composer Nationality
    + Composer Era
    + Imputed Composer Nationality/Era
    + Performer cluster/types
    + Piece seasonality

- Dynamic Weighting:
    + Down-weight recently chosen pieces
    + Exclude for X number of programs and then recover to full weighting over Y generations
