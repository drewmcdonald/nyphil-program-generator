# Notes

## Model Structure

- Training Data: 
    + DataFrame of ConcertSelections (rows)
    + indexed by \[Concert.id, Selection.id\]
    + `weight` field to hold count of performances
    + 1 or more columns of categorical features to model

- Sub-models:
    + Composer Nationality
    + Composer Era
    + Imputed Composer Nationality/Era
    + Performer cluster/types
    + Piece seasonality

- Training:
    + for each categorical feature:
        * split by concert, interpolate begin/end flags
        * optionally prune to feature values with adequate representation
        * train a Markov chain that accumulates counts of state -> outcome, then convert to probabilities

- Prediction:
    + De-dupe training data to unique selections marked for prediction
        * apply model pruning
        * Append record for BREAK
    + for each feature/sub-model:
        * apply (feature=value prediction) divided by (count of feature=value) to each feature=value record
    + result is DataFrame of records with one column per each feature's probability prediction
    + summarize weighted probabilities into a final record probability
        * simple weighted average
        * OR weighted sum of log-odds
        * OR the default, (feature_0^weight_0 + feature_1^weight_1 + ...)^(1/sum(weight))
    + apply nonlinear transformations
        * on final probabilities (vary decisiveness)
        * on case weights (vary appearance of less frequently performed selections)
    + sample a record from the weighted probability sum
    + scrub
        * remove selected record from scoring set
        * remove records sharing certain features (e.g. no more concertos after the first)
        * may use features not present in chains?
    + repeat until BREAK marker predicted


## Model Calibration



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

MBZComposer: MusicBrainz metadata for a composer
- unique per *Composer* record
- not available for all *Composers*
- linked with composer_id field


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

