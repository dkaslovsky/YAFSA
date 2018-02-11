# YAFSA
Yet Another Fantasy Sports Analysis

Everyone thinks (s)he is a fantasy football expert.  Some even claim this title as a profession and publish weekly rankings by position, upon which countless rely for setting lineups.  Using ranking metrics, this project scores the experts' rankings against the actual fantasy points scored by each player every Sunday.  In other words, this project ranks the rankers!

Currently, there are two ranking metrics in use:
* *Discounted Cumulative Gain (DCG)*: https://en.wikipedia.org/wiki/Discounted_cumulative_gain
* *Difference From Rank's Mean Points*: The difference between the points scored by the player ranked in poisition i and the average points scored by players ranked in position i.  Typically, only negative are retained to avoid artificial inflation of the metric by a player who far exceeds the mean.

Example Usage:

```
$ python driver.py
```

```
                  DCG  Diff  Composite
FieldYates          2     1          3
TristanCockcroft    5     2          7
MikeClay            6     3          9
BrandonFunston      4     6         10
HeathCummings       1     9         10
StaffComposite      8     5         13
AndyBehrens         3    12         15
LizLoza             9     7         16
JameyEisenberg     13     4         17
PatrickDaugherty    7    11         18
BradEvans          12     8         20
DaveRichard        10    10         20
RaymondSummerlin   11    15         26
DaltonDelDon       14    14         28
EricKarabell       15    13         28
ScottPianowski     16    16         32
```

We see that (for the 2016 season) Field Yates of ESPN has the best overall composite score, ranking 2nd according to DCG and 1st according to Diff.  Follow his rankings instead of Scott Pianowski of Yahoo to win your league!

<br/>

**To Do**
* Persist historical rankings and points in a database rather than CSV/JSON files, update readers to query DB
* Separate the TableScraper class into its own project since it is sufficiently generic
* Unit tests are needed
