# NYPhil Program Generator

Generate a classical concert program based on the New York Philharmonic's open source [program history](https://github.com/nyphilarchive/PerformanceHistory).

Similar to a text-based Markov Chain, programs are filled based off of selections that have already been performed until a likely endpoint is reached. As one example, a basic Markov model may learn that Beethoven's 5th Symphony is likely to end a program that began with Mozart's clarinet concerto.

However, instead of generating Markov chains based on individual selections (which is prone to over-fitting and tends to hit dead ends via niche selections), the model develops probability estimates based off of individual models of selection meta data. 

So, in the example above, the prediction of Beethoven's 5th Symphony would be the combination of the likelihood of a German composer following an Austrian composer, the probability of a symphony following a wind concerto, and the probability of an early 1800s composition following a late 1700s composition. These individual meta data-based probabilities are combined for all possible following works to a final likelihood (which can be tuned and weighted) out of which a prediction is sampled.

Other meta data may include the selection's or composer's relative popularity, the selection's typical concert order, and other features derived from third party sources like [MusicBrains](https://musicbrainz.org/) or Spotify.

Predictions can be generated as a single standalone concert, or chained together to represent a 'season'. In the future, this 'season' generation could be used to generate a Spotify playlist.

See [notes.md](notes.md) for detailed planning information.
