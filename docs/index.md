# Introduction

Welcome to the technical documentation & reference materials for **[chickenstats](https://github.com/chickenandstats/chickenstats)**,
a package for scraping & analyzing NHL data.

??? tip 
    Navigate the site using the header, side-bar, or search tool.
    Mobile users can tap the :material-menu: (upper-left) to bring up the menu.

<div class="grid cards" markdown>

-   :material-download-box:{ .lg .middle } __Usage & installation__

    ---

    Download & install `chickenstats` with `pip` to get up
    & running in just a few minutes.

    [:octicons-arrow-right-24: Getting started](home/getting_started.md)

-   :material-school:{ .lg .middle } __Tutorials & examples__

    ---

    Discover the package using hands-on tutorials
    & examples from the User Guide.

    [:octicons-arrow-right-24: User Guide](guide/overview.md)

-   :material-bookshelf:{ .lg .middle } __Reference materials__

    ---

    Consult the Reference section for assistance debugging & 
    to explore the package in greater detail.

    [:octicons-arrow-right-24: Reference](home/getting_started.md)

-   :material-progress-wrench:{ .lg .middle } __Contribute__

    ---

    Read the roadmap, double-check known errors, &
    learn how to contribute to `chickenstats`.

    [:octicons-arrow-right-24: Contribute](guide/overview.md)

</div>

## About chickenstats

`chickenstats` is an open-source Python package developed
to facilitate public hockey research & analytics. The goal is to improve 
understanding of the sport with public access to stable, reliable data,
promoting creativity, collaboration, & reproducibility in professional, academic,
& amateur hockey research.

The package has three pillars: :material-numeric-1-circle: `chicken_nhl`
:material-numeric-2-circle: `evolving_hockey` and :material-numeric-3-circle:
`capfriendly`. Each pillar functions individually or can be leveraged in coordination
with the others using synchronized player identification fields.

=== ":material-numeric-1-circle: `chicken_nhl`"

    The functions from `chicken_nhl` scrape data from official NHL sources(1)
    to construct a play-by-play dataframe with 170+ potential fields(2) for each event,(3)
    then aggregates the returned statistics to enable further analysis. Below is a visual representation
    of the `scrape_pbp()` function. Additional functions & information can be found in the **[Reference](#)**
    section of the documentation.
    { .annotate } 

    1.  Sources include (non-exhaustive): :material-numeric-1-circle: HTML shifts, :material-numeric-2-circle: events, &
        :material-numeric-3-circle: rosters, as well as :material-numeric-4-circle: events,
        :material-numeric-5-circle: rosters, & :material-numeric-6-circle: game information
        from the NHL's API endpoints.
    2.  Fields include (non-exhaustive) primary player idenfitication & information
        (e.g., position, age, handedness, size, & weight), various game state characteristics
        (e.g., strength-state, score-state, score differential),
        Cartesian event coordinates, shot type (e.g., wrist, slap, deflection), distance & angle from net,
        & on-ice teammate & opponent identification & information.
    3.  Supported events include: :material-numeric-1-circle: goals (including assists),
        :material-numeric-2-circle: shots on net, :material-numeric-3-circle: missed shots,
        :material-numeric-4-circle: blocked shots, :material-numeric-5-circle: faceoffs,
        :material-numeric-6-circle: penalties & delayed penalties, :material-numeric-7-circle:
        giveaways, & :material-numeric-8-circle: takeaways.

    <div class="center">
    ```mermaid
    graph LR
        subgraph raw[Raw data]
        api_events_raw(API events)
        game_info_raw(Game info)
        api_rosters_raw(API rosters)
        html_rosters_raw(HTML rosters)
        html_events_raw(HTML events)
        shifts_raw(Shifts)
        end

        subgraph intermediate[Intermediate processing]
        api_pbp(API play-by-play)
        rosters(Combined rosters)
        html_pbp(HTML play-by-play)
        changes(Changes on / off)
        end

        subgraph final_scrape[Scraped data]
        final_pbp(Play-by-play dataframe)
        end

        raw --> intermediate --> final_scrape

        api_events_raw & game_info_raw & api_rosters_raw --> api_pbp --> final_pbp

        game_info_raw & api_rosters_raw & html_rosters_raw --> rosters --> final_pbp

        html_rosters_raw & html_events_raw --> html_pbp --> final_pbp

        html_rosters_raw & shifts_raw --> changes --> final_pbp

    ```
    </div>

    `chicken_nhl` also includes functions to scrape the schedule & current standings, in addition
    to the play-by-data.(1) 
    Other functions aggregate the data to individual, line, & team levels, with options 
    to further segment the data by strength state, score state, & on-ice teammates & oppponents.(2)
    { .annotate }

    
    1.  All functions are available to users, including play-by-play inputs (e.g., `scrape_changes()`,
        `scrape_rosters()`, `scrape_api_events()`)
        enabling customized designs & use-cases for your application. 
    2.  For example, who played with whom, for how long, & to what effect in any given game, period, or season,
        at 5v5, down 2 goals? What was the average of their teammates & opponents? What positions were on the ice? 

    ??? info "Data are supported from 2010-11 to present"
        With some exceptions for individual games, the `scrape_pbp()`
        function will return data for games occurring since the start of
        the 2010-2011 season. However, the `scrape_schedule()` & `scrape_standings()`
        functions will return data extending to the NHL's founding in 1917.

=== ":material-numeric-2-circle: `evolving_hockey`"

    Leveraging data from [Evolving Hockey](https://www.evolving-hockey.com), the functions
    from `evolving_hockey` append additional fields to the raw play-by-play queries
    using raw shifts queries. The package contains additional tools to aggregate the
    transformed data to the individual, line, & team levels, with options to further segment
    the data by strength state, score state, & on-ice teammates & oppponents. The purpose is
    to enhance & programmatically recreate various datasets published by
    [Evolving Hockey](https://www.evolving-hockey.com), reducing the time spent clicking
    through the site & collecting raw csv files & increasing the time available for research & analysis.

    ??? warning "Evolving Hockey subscription required for usage"
        You will need an Evolving Hockey subscription, or at least access to their raw
        query csv files to make use of these functions. Minimal data will be provided for
        the tutorials & examples found in the user guide.

    Below are visual representations of the `prep_pbp()` & `prep_stats()` functions.
    Additional functions & information can be found in the **[Reference](#)**
    section of the documentation.

    ```mermaid  
    graph LR
        subgraph pbp[prep_pbp]
        raw_pbp(Raw play-by-play query)
        processed_pbp(Processed play-by-play)
        raw_shifts(Raw shifts query)
        rosters(Rosters extracted)
        final_pbp(Play-by-play dataframe)
        end

        raw_pbp --> processed_pbp --> final_pbp
        raw_shifts --> rosters --> final_pbp

        subgraph stats[prep_stats]
        prepped_pbp(Play-by-play dataframe)
        individual(Individual stats)
        on_ice(On-ice stats)
        goalies(Goalie stats)
        final_stats(Stats dataframe)
        end

        pbp --> stats

        prepped_pbp --> individual & on_ice & goalies --> final_stats
    ```

=== ":material-numeric-3-circle: `capfriendly`"

    The functions from `capfriendly` will scrape & munge the data from the active
    players portion of the Capfriendly site, including fields like AAV, trade protections,
    arbitration status, & contract length. 

    ???+ warning "Please scrape respectfully & responsibly"
        Capfriendly provides a truly invaluable service to NHL & hockey fans.
        Be sure to save when scraping to avoid unneccessary hits to their servers.

## Acknowledgements

`chickenstats` would not be possible without the efforts of countless other individuals.
