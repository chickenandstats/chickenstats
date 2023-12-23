---

icon: material/download-box

---

# :material-download-box: **Getting started**

Instructions for installing `chickenstats`, basic usage, and downloading example guides.

## :material-book: **Requirements**

`chickenstats` requires Python 3.7 or later, preferably the most recent version (3.10).

## :material-download: **Installation**

`chickenstats` can be installed via PyPi:

```py
pip install chickenstats
```

You can ensure the install was successful by checking that you have the latest version (1.8.0) installed:

```py
pip show chickenstats
```

## :fontawesome-solid-user-large: **Basic usage**

Once installed in your preferred environment, you can immediately begin using `chickenstats` in your preferred IDE.
First, import the package and the relevant functions:

```py
from chickenstats.chicken_nhl import scrape_schedule, scrape_standings, scrape_pbp
```

Then, you're off. The `scrape_schedule` function will return NHL game IDs, which are the foundation of almost all 
`chicken_nhl` functionality. The `scrape_pbp` function will return approximately one game every 2.5 seconds, 
with no loss in performance when scraping hundreds (or thousands of games). The following snippet will scrape
the entire current season's finished games:

```py
sched = scrape_schedule(2022, final_only = True) 

game_ids = sched.game_id

pbp = scrape_pbp(game_ids)
```

If you wanted to scrape live games only:

```py
sched = scrape_schedule(live_only = True)

game_ids = sched.game_id

pbp = scrape_pbp(game_ids)
```

The `scrape_standings` function will return the latest NHL standings, as well as division, conference, and wild card rankings:

```py
standings = scrape_standings(2022)
```

## :material-school: **Tutorials & examples**






