
# RTCWPro Stats developer doc

This project is build to collect RTCWPro stats submitted by servers after the end of each round. The solution is built in a modern pipeline design fashion where data is:
1. Ingested -> retained
2. Pre-processed -> stored
3. Cleansed/tagged/post processed
4. Exposed to final consumer via REST API

## Architecture
**Conceptual flow**
![alt text](./readme-img-concept.png "Conceptual Flow")

**Technical architecture**
![alt text](./readme-img-architecture.png "AWS Components Flow")

The project relies on AWS CDK framework that allows deploying infrastructure as code(IaC).
This way all deployed resources (storgaes, databases, jobs) are robustly controlled, versioned, and easily replicated. 

## Pipeline components
* RTCW Pro servers submit the end round stats to the ingestion API
* **Save_payload** lambda python job accepts the json payload and stores it in permanent file raw storage
* Event driven **read-match** lambda python job picks up the incoming file and with minimal processing puts it into the database
* A series of sequential lambda python jobs process the results, perform calculations, elo, awards, etc **<-- crowdsourcing potential here**
* Rest of the API uses the **Retriever** lambda python job to process end user requests to return json with players, matches, stats, elo, etc. Work in progress
* Web/discord/rtcwpro developers consume the API to build applications

## Outputs 
End users (app developers and final consumers) have access to the following APIs
https://rtcwproapi.donkanator.com/matches/16098173561
* rtcwproapi.donkanator.com is API domain
* and /matches/16098173561 is API path represented below as /matches/{match_round_id}

The contents of the json should be mostly familiar to anyone with RTCW game knowledge.
Extra fields specific to this project: 
match_type represented with string <region>#<type> - ex. na#6 is north america 6v6
jsonGameStatVersion - RTCW Pro version of json

Important difference to keep in mind - matches and gamelogs are identified by {match_round_id}, and stats and wstats are identified with {match_id}. Example: 
/match/16098173561 is match 1609817356 round 1
/stats/1609817356 comes wihout "1" at the end.

The following APIs are available at the time of writing this documentation:
x is done , / is work in progress, blank is planned

|Status  |Domain  |API Path  |Query  |
|-------- |-------- |-------- |--------|
|[x] |Matches |/matches/{match_round_id} |Filter matches |
| |Matches |/matches/map/{map} |Filter matches by map to get map counts, wins, times |
| |Matches |/matches/type/{type} |Filter type of matches and process their individual lines |
|[x] |Matches |/matches/recent |Filter by last 30 days to get matches |
|[x] |Matches |/matches/recent/{days} |Filter by last x days to get recent matches |
|[x] |Gamelog |/gamelogs/{match_round_id} |Retrieve game log for a match |
|[x] |Stats |/stats/player/{guid} |Filter stats for a player guid from many matches |
|[x] |Stats |/stats/{matchid} |Filter stats for a match or range of matches |
| |Stats |/stats/type/{type} |Filter stats for a match type on range of matches |
|[x] |Weapons |/wstats/{matchid} |By match retrieve all wstats |
|[x] |Weapons |/wstats/player/{player_guid}/match/{matchid} |By player by match |
|[x] |Weapons |/wstats/player/{player_guid} |By player by several matches |
| |Players |/player/{guid}/aliases |Filter by guid to get aliases range and dates |
| |Players |/player/{guid}/primaryalias |Filter by given guid to get primary name |
| |Players |/player/{guid}/elo/{type} |Filter by given guid to get current elo |
| |Players |/player/{guid}/primaryregion |Filter by given guid to get primary region |
| |Players |/player/{guid}/weaponaccuracy/{weapon} |Filter by given guid to get weapon stats |
| |Players |/player/{guid}/eloprogress/{type} |Filter by guid sort by time to get elo progress |
| |Players |/player/recent |Filter by last 30 days to get primary aliases |

Example: 
https://rtcwproapi.donkanator.com/matches/recent/92
will (maybe) give match 16098173561, therefore stats and wstats will follow:
https://rtcwproapi.donkanator.com/stats/1609817356
https://rtcwproapi.donkanator.com/wstats/1609817356
https://rtcwproapi.donkanator.com/wstats/player/A53B3ED2A896CB/match/1609817356

## Usage 
* Familiarize yourself with what's available in the API section
* Talk to donkz about your plan to consume the API
* Work from /matches down to /stats by match_id or player_guid
* Assume jsonGameStatVersion may change and new fields may appear
* Assume {error: "error message can be returned"}

## Contribution to the pipeline
There's plentry of potential to contribute. Currently looking for post-processing calculations for:
* gamelogs insights
* stats aggregation
* wstats aggregation
* more APIs
* ELO 

https://rtcwproapi.donkanator.com/gamelogs/16098173561
https://rtcwproapi.donkanator.com/stats/1609817356
Can you make sense of these events? Can you come up with a plan to process json and store results?
Can you work towards making your results available to final consumer?

Where to start:
The project currently uses python, but I'm open to Java, Go, PowerShell, Node. js, C#, Python, and Ruby, as long as you are willing to support it.
* Understand what you want to do: final consumer presentation or inner pipeline calc
* Save or call a particular API like https://rtcwproapi.donkanator.com/gamelogs/16098173561
* Ingest JSON and do your thing
* Make a plan what needs to be saved/cached


