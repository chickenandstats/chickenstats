############################################## Introduction ##############################################

# Welcome to the chicken_nhl scraper functions

# All credit to Drew Hynes and his nhlapi project (https://gitlab.com/dword4/nhlapi)

# The two most important functions are: (1) scrape_schedule; and (2) scrape_pbp
# The play-by-play function takes game IDs, which can be sourced using the schedule scraper

############################################## Dependencies ##############################################

import requests
from requests.adapters import HTTPAdapter
from requests import ConnectionError, ReadTimeout, ConnectTimeout, HTTPError, Timeout
import urllib3

from bs4  import BeautifulSoup

from datetime import datetime
from datetime import timedelta
from pytz import timezone

import pandas as pd
import numpy as np

from tqdm.auto import tqdm

import unidecode
import re

# These are dictionaries of names that are used throughout the module
from chickenstats.chicken_nhl.info import correct_names_dict, correct_api_names_dict, team_codes

############################################## Requests functions & classes ##############################################

# This function & the timeout class are used for scraping throughout

class TimeoutHTTPAdapter(HTTPAdapter):
    
    def __init__(self, *args, **kwargs):
        
        self.timeout = 3

        if "timeout" in kwargs:

            self.timeout = kwargs["timeout"]

            del kwargs["timeout"]

        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):

        timeout = kwargs.get("timeout")

        if timeout is None:

            kwargs["timeout"] = self.timeout

        return super().send(request, **kwargs)

def s_session():
    '''Creates a requests Session object using the HTTPAdapter from above'''

    s = requests.Session()
    
    retry = urllib3.Retry(total = 5, backoff_factor = 1, respect_retry_after_header = False,
                          status_forcelist=[60, 401, 403, 404, 408, 429, 500, 502, 503, 504])
    
    adapter = TimeoutHTTPAdapter(max_retries = retry, timeout = 3)
    
    s.mount('http://', adapter)
    
    s.mount('https://', adapter)
    
    return s

############################################## General helper functions ##############################################

# These are used in other functions throughout the module

def scrape_live_endpoint(game_id, session):
    '''Scrapes the live NHL API endpoint. Used to prevent multiple hits to same endpoint during PBP scrape'''

    s = session
    
    url = f'https://statsapi.web.nhl.com/api/v1/game/{game_id}/feed/live'
    
    response = s.get(url).json()
    
    return response

def convert_to_list(obj, object_type):
    '''If the object is not a list, converts the object to a list of length one'''

    if type(obj) is str or type(obj) is int or type(obj) is float:

        obj = [obj]

    else:

        try:

            obj = [x for x in obj]

        except:

            raise Exception(f"'{obj}' not a supported {object_type} or range of {object_type}s")
    
    return obj

def hs_strip_html(td):
    """
    Function from Harry Shomer's Github, which I took from Patrick Bacon
    
    Strip html tags and such 
    
    :param td: pbp
    
    :return: list of plays (which contain a list of info) stripped of html
    """
    for y in range(len(td)):
        # Get the 'br' tag for the time column...this get's us time remaining instead of elapsed and remaining combined
        if y == 3:
            td[y] = td[y].get_text()   # This gets us elapsed and remaining combined-< 3:0017:00
            index = td[y].find(':')
            td[y] = td[y][:index+3]
        elif (y == 6 or y == 7) and td[0] != '#':
            # 6 & 7-> These are the player 1 ice one's
            # The second statement controls for when it's just a header
            baz = td[y].find_all('td')
            bar = [baz[z] for z in range(len(baz)) if z % 4 != 0]  # Because of previous step we get repeats...delete some

            # The setup in the list is now: Name/Number->Position->Blank...and repeat
            # Now strip all the html
            players = []
            for i in range(len(bar)):
                if i % 3 == 0:
                    try:
                        name = return_name_html(bar[i].find('font')['title'])
                        number = bar[i].get_text().strip('\n')  # Get number and strip leading/trailing newlines
                    except KeyError:
                        name = ''
                        number = ''
                elif i % 3 == 1:
                    if name != '':
                        position = bar[i].get_text()
                        players.append([name, number, position])

            td[y] = players
        else:
            td[y] = td[y].get_text()

    return td

def convert_ids(api_game_id):
    '''Takes an NHL API ID and converts it to an HTML season and game ID'''

    html_season_id = str(int(str(api_game_id)[:4])) + str(int(str(api_game_id)[:4]) + 1)
    
    html_game_id = str(api_game_id)[5:]
    
    return html_season_id, html_game_id

############################################## Schedule ##############################################

## [x]Refactored
def scrape_schedule(seasons = 2022, game_types = ['R', 'P'], date = None, final_only = False, live_only = False, teams = None, disable_print = False):
    
    
    '''
    
    --------- Info ---------

    Scrapes the NHL schedule API and returns a DataFrame for the given year or years.
    By default, returns entire 2022 schedule, including playoffs.
    NHL historical data supported, 1917-present.
    Typically takes 2-5 seconds per seasons, but can increase if scraping multiple seasons. 

    --------- Parameters ---------

    seasons: a four-digit year, or list-like object consisting of four-digit years

    game_types: list; default: ['R', 'P']
        Determines the types of games that are returned. Not all game types are supported or have adequate data.
        The following are available:
        'R': regular season
        'P': playoffs
        'PR': preseason (not supported)
        'A': all star game (not supported)
        'all': all game types

    date: Date in 'YYYY-MM-DD' format or 'today'; default: None
        If not None, scrapes games from the date given, or today, in your system's local time

    final_only: boolean; default: False
        If True, scrapes only the games that have finished

    live_only: boolean; default: False
        If True, scrapes only live games

    teams: list of team names; default: None
        If not none, filters the schedule to include only the given teams

    disable_print: boolean; default: False
        If False, prints progress to the console using tqdm

    nested: boolean; default: False
        If False, closes session object when finished

    --------- Returns ---------
    
    Pandas DataFrame with columns:
        
        1. season: string
            Season as 8-digit number, e.g., 20222023 for 2022-23 season

        2. game_id: integer
            Unique game ID assigned by the NHL

        3. game_date_dt: datetime
            Datetime in Eastern time zone for game start

        4. game_date: string
            Date game is / was played (Eastern time)

        5. start_time: string
            Time game is / was started (Eastern time)
    
        6. game_type: string
            Whether game is regular season, playoffs, or other, e.g., all-star

        7. game_status: string
            Whether game is final, currently being played, or scheduled

        8. home_team: string
            Team name in upper case, no accents (sorry MONTREAL)

        9. home_team_code: string
            Three-letter team code

        10. home_team_score: integer
            Goals scored by home team - shootout win is a goal

        11. away_team: string:
            Team name in upper case, no accents (sorry MONTREAL)

        12. away_team_code: string
            Three-letter team code

        13. away_team_score: string
            Goals scored by away team - shootout win is a goal

        14. detailed_game_status: string
            Whether game is final, currently being played, or scheduled

        15. start_time_tbd: bool
            If start time is to be determined, then True

        16. home_team_wins: integer
            Number of wins by home team entering game if not yet played, and exiting game if already played

        17. home_team_losses: integer
            Number of losses by home team entering game if not yet played, and exiting game if already played

        18. home_team_otl: integer
            Number of overtime losses by home team entering game if not yet played, and exiting game if already played

        19. away_team_wins: integer
            Number of wins by away team entering game if not yet played, and exiting game if already played

        20. away_team_losses: integer
            Number of losses by away team entering game if not yet played, and exiting game if already played

        21. away_team_otl: integer
            Number of overtime losses by away team entering game if not yet played, and exiting game if already played

        22. home_team_id: integer
            Unique team / franchise identifier given by NHL

        23. home_team_link: string
            Link to team information via the NHL API

        24. away_team_id: integer
            Unique team / franchise identifier given by NHL

        25. away_team_link: integer
            Link to team information via the NHL API

        26. venue_name: string
            Name of the venue where game is played 

        27. venue_id: string
            Unique venue identifier given by the NHL. Will be np.nan if not a venue with regular games

        28. venue_link: string
            Link to venue information via the NHL API. Will be null if not a venue with regular games

        29. game_link: string
            Link to detailed game information via the NHL API 

        30. game_content_link: string
            Link to additional game content via the NHL API

        31. status_code: string
            Code to indicate whether game has been played, is currently being played, or is scheduled 

    '''
    
    ## Starting requests session
    s = s_session()
    
    ## Create list of season IDs. If single season, convert to a list of a single season ID. Else, use list comprehension to convert to season IDs
    seasons = convert_to_list(obj = seasons, object_type = 'season')
        
    season_list = []
    
    pbar = tqdm(seasons, disable = disable_print)
    
    for season in pbar:
        
        if season == 2004:

            pbar.set_description(f'{season} cancelled due to lockout')
        
            now = datetime.now()

            current_time = now.strftime("%H:%M:%S")

            postfix_str = f'{current_time}'
            
            pbar.set_postfix_str(postfix_str)
            
            continue
        
        ## Create season ID for each season
        season_id = str(int(season)) + str(int(season) + 1) 
        
        eastern = timezone('US/Eastern')
        
        today = datetime.now(eastern)
    
        if date == 'today':
            
            date = today.strftime('%Y-%m-%d')

        if date == 'yesterday':
            
            date = (today - timedelta(days = 1)).strftime('%Y-%m-%d')
            
        if date is not None:

            url = f'https://statsapi.web.nhl.com/api/v1/schedule?date={date}'

        ## Setting url and calling JSON from API
        else:

            url = f'https://statsapi.web.nhl.com/api/v1/schedule?season={season_id}'

        response = s.get(url, timeout = 1).json()
        
        ## Setting up initial season schedule dataframe
        season_df = pd.json_normalize(response['dates'], record_path = 'games', sep = '_')

        season_df['season'] = season_id
        
        ## Removing game types based on function argument. Game types dictionary is function argument.
        ## By default, only regular season and playoff game types are included
        
        if game_types != 'all':
            
            season_df = season_df[np.isin(season_df.gameType, game_types)].copy()
        
        ## Removing unfinished/unplayed games from sched sule information. By default live games are included
        if final_only == True:
            
            season_df = season_df[season_df.status_abstractGameState == 'Final'].copy()
            
        if live_only == True:
            
            season_df = season_df[season_df.status_abstractGameState == 'Live'].copy()
        
        season_df.teams_away_team_name = season_df.teams_away_team_name.str.normalize('NFKD')\
                                                    .str.encode('ascii', errors='ignore').str.decode('utf-8').str.upper()

        season_df.teams_home_team_name = season_df.teams_home_team_name.str.normalize('NFKD')\
                                                    .str.encode('ascii', errors='ignore').str.decode('utf-8').str.upper()

        season_df = season_df.replace({'PHOENIX COYOTES': 'ARIZONA COYOTES'}, regex = False)

        season_df['game_date_dt'] = pd.to_datetime(season_df.gameDate).dt.tz_convert('US/Eastern')
        
        season_df['start_time'] = season_df.game_date_dt.dt.strftime("%H:%M")
        
        season_df.gameDate = season_df.game_date_dt.dt.strftime('%Y-%m-%d')
        
        cols = ['home', 'away']
        
        for col in cols:
            
            season_df[f'{col}_team_code'] = season_df[f'teams_{col}_team_name'].map(team_codes)

        if teams != None:

            teams = convert_to_list(teams)

            mask = np.logical_or(np.isin(season_df.teams_home_team_name, teams), np.isin(season_df.teams_away_team_name, teams))

            season_df = season_df[mask].copy()

        #season_df.gameDate = pd.to_datetime(season_df.gameDate).dt.tz_convert('US/Eastern').dt.date

        ## Adding individual season schedule to big schedule dataframe
        season_list.append(season_df)
        
        if season == seasons[-1]:

            if len(seasons) == 1:

                season_length = season

            elif str(seasons[0])[0:2] != str(seasons[-1])[0:2]:

                season_length = f'{seasons[0]}-{seasons[-1]}'

            elif len(seasons) > 1:

                season_length = f'{seasons[0]}-{str(seasons[-1])[-2:]}'
            
            pbar.set_description(f'Finished scraping schedule data ({season_length})')
            
        else:
        
            pbar.set_description(f'Finished scraping {season}-{season + 1}')
        
        now = datetime.now()

        current_time = now.strftime("%H:%M:%S")

        postfix_str = f'{current_time}'
        
        pbar.set_postfix_str(postfix_str)
        
    try:
    
        df = pd.concat(season_list, ignore_index = True)
        
    except:
        
        return pd.DataFrame()
    
    ## Changing column names using dictionary and rename function
    new_cols = {'gamePk' : 'game_id',
                'gameType' : 'game_type',
                'link' : 'game_link',
                'gameDate' : 'game_date',
                'status_abstractGameState' : 'game_status',
                'status_detailedState' : 'detailed_game_status',
                'status_startTimeTBD' : 'start_time_tbd',
                'teams_away_leagueRecord_wins' : 'away_team_wins',
                'teams_away_leagueRecord_losses' : 'away_team_losses',
                'teams_away_leagueRecord_ties': 'away_team_ties',
                'teams_away_leagueRecord_ot' : 'away_team_otl',
                'teams_away_score' : 'away_team_score',
                'teams_away_team_id' : 'away_team_id',
                'teams_away_team_name' : 'away_team',
                'teams_away_team_link' : 'away_team_link',
                'teams_home_leagueRecord_wins' : 'home_team_wins',
                'teams_home_leagueRecord_losses' : 'home_team_losses',
                'teams_home_leagueRecord_ot' : 'home_team_otl',
                'teams_home_leagueRecord_ties': 'home_team_ties',
                'teams_home_score' : 'home_team_score',
                'teams_home_team_id' : 'home_team_id',
                'teams_home_team_name' : 'home_team',
                'teams_home_team_link' : 'home_team_link',
                'content_link' : 'game_content_link', 
                'status_statusCode': 'status_code'}

    df.rename(columns = new_cols, inplace = True)
    
    ## Dropping columns that we don't want
    
    columns = ['season', 'game_id', 'game_date_dt', 'game_date', 'start_time', 'game_type', 'game_status', 'home_team', 'home_team_code',
               'home_team_score', 'away_team', 'away_team_code', 'away_team_score', 'detailed_game_status',
               'start_time_tbd', 'home_team_wins', 'home_team_losses', 'home_team_ties', 'home_team_otl', 'away_team_wins',
               'away_team_losses', 'away_team_otl', 'home_team_id', 'home_team_link', 'away_team_id', 'away_team_link',
               'venue_name', 'venue_id', 'venue_link', 'game_link', 'game_content_link', 'status_code']
    
    columns = [x for x in columns if x in df.columns]
    
    df = df[columns]#.convert_dtypes()

    s.close()
        
    return df

############################################## Standings ##############################################

## [x]Refactored
def scrape_standings(seasons = 2022, disable_print = False):
    
    '''
    
    --------- Info ---------

    Scrapes the NHL standings API and returns a DataFrame for the given year or years.
    By default, the standings are for 2022 and are of the moment the function is run,
    e.g., anything in the past will be historical.
    NHL historical data supported, 1917-present. 
    Typically takes ~1 second per season.

    --------- Parameters ---------

    seasons: a four-digit year, or list-like object consisting of four-digit years

    disable_print: boolean; default: False
        If False, prints progress to the console using tqdm

    --------- Returns ---------

    Pandas DataFrame with columns:
        
        1. season: integer
            Season as 8-digit number, e.g., 20222023 for 2022-23 season

        2. team: string
            Team name in upper case, no accents (sorry MONTREAL)

        3. team_code: string
            Three-letter team code
    
        4. games_played: integer
            Total number of games played

        5. points: integer
            Standings points earned

        6. points_percentage: float
            Standings points earned as a percentage of total standings points possible

        7. win: integer
            Number of wins earned by team

        8. loss: integer
            Number of losses incurred by team

        9. otl: integer
            Number of overtime losses incurred by team

        10. streak: string
            Streak of games team has lost, won, or tied

        11. league_rank: integer
            League rank by points percentage

        12. conference_rank: integer
            Conference rank by points percentage

        13. division_rank: integer
            Division rank by points percentage

        14. wildcard_rank: integer
            Ranking for wild card playoff spot

        15. goals_scored: integer
            Number of goals scored

        16. goals_against: integer
            Number of goals allowed

        17. team_id: integer
            Unique team ID assigned by the NHL

        18. team_link: integer
            Link to team information via the NHL API

        19. division_rank_home: integer
            Division ranking by home points percentage

        20. division_rank_road: integer
            Division ranking by road points percentage

        21. division_rank_last10: integer
            Division ranking by points percentage in the last ten games

        22. conference_rank_home: integer
            Conference ranking by home points percentage

        23. conference_rank_road: integer
            Conference ranking by road points percentage

        24. conference_rank_last10: integer
            Conference ranking by points percentage in the last ten games

        25. league_rank_home: integer
            League ranking by home points percentage

        26. league_rank_road: integer
            League ranking by road points percentage

        27. league_rank_last10: integer
            League ranking by points percentage in the last ten games

        28. pp_rank_division: integer
            Powerplay ranking within division

        29. pp_rank_conference: integer
            Powerplay ranking within conference

        30. pp_rank_league: integer
            Powerplay ranking within league

        31. last_updated: string
            Datetime standings were last updated

    '''

    ## Starting requests session
    s = s_session()
    
    ## Create list of season IDs. If single season, convert to a list of a single season ID. Else, use list comprehension to convert to season IDs
    seasons = convert_to_list(obj = seasons, object_type = 'season')
        
    standings_list = []
    
    pbar = tqdm(seasons, disable = disable_print)
    
    for season in pbar:
        
        if season == 2004:

            pbar.set_description(f'{season} cancelled due to lockout')
        
            now = datetime.now()

            current_time = now.strftime("%H:%M:%S")

            postfix_str = f'{current_time}'
            
            pbar.set_postfix_str(postfix_str)
            
            continue

        url = f'https://statsapi.web.nhl.com/api/v1/standings?season={season}{season + 1}'

        r = s.get(url)

        concat_list = []

        for division in r.json()['records']:

            concat_list.append(pd.json_normalize(division['teamRecords'], sep = '_'))

        new_cols = {'team_name': 'team', #'regulationWins': 'regulation_wins',
                    'goalsAgainst': 'goals_against', 'goalsScored': 'goals_scored',
                    'divisionRank': 'division_rank', 'divisionL10Rank': 'division_rank_last10',
                    'divisionRoadRank': 'division_rank_road', 'divisionHomeRank': 'division_rank_home',
                    'conferenceRank': 'confrence_rank', 'conferenceL10Rank': 'conference_rank_last10',
                    'conferenceRoadRank': 'conference_rank_road', 'conferenceHomeRank': 'conference_rank_home',
                    'leagueRank': 'league_rank', 'leagueL10Rank': 'league_rank_last10', 'leagueRoadRank': 'league_rank_road',
                    'leagueHomeRank': 'league_rank_home', 'wildCardRank': 'wildcard_rank', 'gamesPlayed': 'games_played',
                    'pointsPercentage': 'points_percentage', 'ppDivisionRank': 'pp_rank_division', 'ppConferenceRank': 'pp_rank_conference',
                    'ppLeagueRank': 'pp_rank_league', 'lastUpdated': 'last_updated', 'leagueRecord_wins': 'win',
                    'leagueRecord_losses': 'loss', 'leagueRecord_ot': 'otl', 'streak_streakCode': 'streak'}
        
        #new_cols = {k: v for k, v in new_cols.items() if k in 

        standings = pd.concat(concat_list, ignore_index = True).rename(columns = new_cols).copy()

        standings['season'] = int(f'{season}{season + 1}')

        cols = ['season', 'team_id', 'team', 'team_link', 'games_played', 'points', 'points_percentage', 'win', 'loss', 'otl',
                'regulation_wins', 'league_rank', 'confrence_rank', 'division_rank', 'wildcard_rank',
                'goals_scored', 'goals_against', 'division_rank_home', 'division_rank_road',
                'division_rank_last10', 'conference_rank_home', 'conference_rank_road',
                'conference_rank_last10', 'league_rank_home', 'league_rank_road', 'league_rank_last10',
                'pp_rank_division', 'pp_rank_conference', 'pp_rank_league', 'streak', 'last_updated']

        cols = [x for x in cols if x in standings.columns]

        standings = standings[cols]

        bad_cols = ['team', 'team_link', 'streak', 'last_updated']

        update_cols = [x for x in standings.columns if x not in bad_cols]

        for col in update_cols:

            standings[col] = pd.to_numeric(standings[col])

        standings.team = standings.team.str.upper().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

        standings = standings.sort_values(by = 'league_rank').reset_index(drop = True)

        #sched = scrape_schedule(year, disable_print = True)

        #teams_dict = dict(zip(sched.home_team_name.unique(), sched.home_team_code.unique()))

        standings['team_code'] = standings.team.map(team_codes)

        cols = ['season', 'team', 'team_code', 'games_played', 'points', 'points_percentage',
                'win', 'loss', 'otl', 'streak',
                'regulation_wins', 'league_rank', 'confrence_rank', 'division_rank', 'wildcard_rank',
                'goals_scored', 'goals_against', 'team_id', 'team_link', 'division_rank_home', 'division_rank_road',
                'division_rank_last10', 'conference_rank_home', 'conference_rank_road',
                'conference_rank_last10', 'league_rank_home', 'league_rank_road', 'league_rank_last10',
                'pp_rank_division', 'pp_rank_conference', 'pp_rank_league', 'last_updated']
                    
        cols = [x for x in cols if x in standings.columns]

        standings = standings[cols]
        
        standings_list.append(standings)

        if season == seasons[-1]:

            if len(seasons) == 1:

                season_length = season

            elif str(seasons[0])[0:2] != str(seasons[-1])[0:2]:

                season_length = f'{seasons[0]}-{seasons[-1]}'

            elif len(seasons) > 1:

                season_length = f'{seasons[0]}-{str(seasons[-1])[-2:]}'
            
            pbar.set_description(f'Finished scraping standings data ({season_length})')
            
        else:
        
            pbar.set_description(f'Finished scraping {season}-{season + 1}')
        
        now = datetime.now()

        current_time = now.strftime("%H:%M:%S")

        postfix_str = f'{current_time}'
        
        pbar.set_postfix_str(postfix_str)

    s.close()
        
    standings = pd.concat(standings_list, ignore_index = True)

    return standings

############################################## Game info ##############################################

## [x]Refactored
def scrape_game_info(game_ids, live_response = None, disable_print = False, session = None, nested = False):
    
    '''

    --------- Info ---------

    Scrapes the game information from the API for a given game ID or list of game IDs. Returns a dataframe.
    Can be used standalone but is typically nested within other scraping functions.
    Typically takes <1 second per game.

    --------- Parameters ---------

    game_ids: a single API game ID (e.g., 2021020001) or list of game IDs

    live_response: JSON object; default: None
        When using in another scrape function, can pass the live endpoint response as a JSON object to prevent redundant hits

    disable_print: boolean; default: False
        If True, prints progress to the console

    session: requests Session object; default = None
        When using in another scrape function, can pass the requests session to improve speed
    '''
    
    ## TO DO:
    ## 1. add comments and edit docstring
    ## 2. ensure columns returned are in the right order
    
    ## Convert game IDs to list if given a single game ID
    game_ids = convert_to_list(obj = game_ids, object_type = 'game ID')
    
    number_of_games = len(game_ids)
    
    ## Important lists
    bad_game_list = []
        
    if session == None:

        s = s_session()

    else:

        s = session
    
    game_list = []
    
    pbar = tqdm(game_ids, disable = disable_print)
    
    for game_id in pbar:
            
        #if np.logical_or(response == None, number_of_games > 1):
        if live_response == None:
            
            response = s.get(f'https://statsapi.web.nhl.com/api/v1/game/{game_id}/feed/live').json()

        else:

            response = live_response
        
        if response['gameData'] == []:
                
            bad_game_list.append(game_id)
            
            response = None
            
            continue
        
        info_list = []
        
        for key in response['gameData'].keys():
            
            info = pd.json_normalize(response['gameData'][key], sep = '_')

            if key == 'game':

                column_names = {'pk': 'game_id', 'type': 'game_type'}

                info = info.rename(columns = column_names)

                info_list.append(info)

            if key == 'datetime':

                column_names = {'dateTime': 'game_start_time', 'endDateTime': 'game_end_time'}

                info = info.rename(columns = column_names)
                
                #info['game_date'] = pd.to_datetime(info.game_start_time, format = '%Y-%m-%d').dt.date

                info_list.append(info)

            if key == 'venue':

                column_names = {'id': 'game_venue_id', 'name': 'game_venue_name', 'link': 'game_venue_link'}

                info = info.rename(columns = column_names)

                info_list.append(info)
        
        info_df = pd.concat(info_list, axis = 1)
        
        ## Creating initial teams dataframe
        teams_dict = {}
                
        for key in response['gameData']['teams'].keys():

            team_df = pd.json_normalize(response['gameData']['teams'][key], sep = '_')

            column_names = {'id': 'team_id', 'name': 'team_name', 'link': 'team_link', 'abbreviation': 'team_abbr',
                            'triCode': 'team_tri_code', 'teamName': 'team_mascot', 'locationName': 'team_location',
                            'firstYearOfPlay': 'team_first_year_of_play', 'shortName': 'team_short_name',
                            'officialSiteUrl': 'team_site_url', 'franchiseId': 'team_franchise_id', 'active': 'team_franchise_active',
                            'venue_id': 'team_venue_id', 'venue_name': 'team_venue_name', 'venue_link': 'team_venue_link',
                            'venue_city': 'team_venue_city', 'venue_timeZone_id': 'team_venue_tz_id',
                            'venue_timeZone_offset': 'team_venue_tz_offset', 'venue_timeZone_tz': 'team_venue_tz_name',
                            'division_id': 'team_division_id', 'division_name': 'team_division_name', 'division_link': 'team_division_link',
                            'conference_id': 'team_conference_id', 'conference_name': 'team_conference_name',
                            'conference_link': 'team_conference_link', 'franchise_link': 'team_franchise_link',
                            'franchise_teamName': 'team_franchise_mascot', 'division_nameShort': 'team_div_short_name'}

            team_df = team_df.rename(columns = column_names).drop('franchise_franchiseId', axis = 1, errors = 'ignore')

            team_df.team_name = team_df.team_name.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.upper()

            team_df.team_name = np.where(team_df.team_name == 'PHOENIX COYOTES', 'ARIZONA COYOTES', team_df.team_name)

            team_df.team_abbr = np.where(team_df.team_abbr == 'PHX', 'ARI', team_df.team_abbr)
            
            if 'team_tri_code' not in team_df.columns:
            
                team_df['team_tri_code'] = np.nan

            team_df.team_tri_code = np.where(team_df.team_tri_code == 'PHX', 'ARI', team_df.team_tri_code)

            team_df['status'] = key

            teams_dict.update({key: team_df})

        teams_list = list()

        merge_dict = {'home': 'away', 'away': 'home'}

        for key, value in merge_dict.items():

            column_names = {x: 'opp_' + x for x in teams_dict[value].columns}
            
            teams_list.append(teams_dict[key].merge(teams_dict[value].rename(columns = column_names),
                                                    left_index = True, right_index = True))
            
        teams_df = pd.concat(teams_list)
        
        teams_df = pd.concat([info_df, teams_df], axis = 1).reset_index(drop = True)

        teams_df['game_id'] = game_id
        
        teams_df['season'] = str(game_id)[:4] + str(int(str(game_id)[:4]) + 1)
        
        teams_df['game_date_dt'] = pd.to_datetime(teams_df.game_start_time).dt.tz_convert('US/Eastern')
        
        teams_df['start_time'] = teams_df.game_date_dt.dt.strftime("%H:%M")
        
        teams_df['game_date'] = teams_df.game_date_dt.dt.strftime('%Y-%m-%d')

        game_list.append(teams_df)
        
        if game_id == game_ids[-1]:
            
            pbar.set_description(f'Finished scraping game info data')
            
        else:
        
            pbar.set_description(f'Finished scraping {game_id}')
        
        now = datetime.now()

        current_time = now.strftime("%H:%M:%S")

        postfix_str = f'{current_time}'
        
        pbar.set_postfix_str(postfix_str)
    
    if game_list != []:

        game_info = pd.concat(game_list, ignore_index = True)
        
        columns = ['game_id', 'season', 'game_type', 'game_date_dt', 'game_date', 'start_time', 'team_id', 'team_name',
                   'team_link', 'team_abbr', 'team_tri_code', 'team_mascot', 'team_location',
                   'team_first_year_of_play', 'team_short_name', 'team_site_url', 'team_franchise_id',
                   'team_franchise_active', 'team_venue_id', 'team_venue_name', 'team_venue_link',
                   'team_venue_city', 'team_venue_tz_id', 'team_venue_tz_offset', 'team_venue_tz_name',
                   'team_division_id', 'team_division_name', 'team_division_link', 'team_conference_id',
                   'team_conference_name', 'team_conference_link', 'team_franchise_mascot', 'team_franchise_link',
                   'status', 'opp_team_id', 'opp_team_name', 'opp_team_link', 'opp_team_abbr', 'opp_team_tri_code',
                   'opp_team_mascot', 'opp_team_location', 'opp_team_first_year_of_play', 'opp_team_short_name',
                   'opp_team_site_url', 'opp_team_franchise_id', 'opp_team_franchise_active', 'opp_team_venue_id',
                   'opp_team_venue_name', 'opp_team_venue_link', 'opp_team_venue_city', 'opp_team_venue_tz_id', 
                   'opp_team_venue_tz_offset', 'opp_team_venue_tz_name', 'opp_team_division_id', 'opp_team_division_name',
                   'opp_team_division_link', 'opp_team_conference_id', 'opp_team_conference_name', 'opp_team_conference_link',
                   'opp_team_franchise_mascot', 'opp_team_franchise_link', 'opp_status']
        
        columns = [x for x in columns if x in game_info.columns]
        
        game_info = game_info[columns]

    if nested == False:

        s.close()
    
    if game_list == []:
        
        return pd.DataFrame()
    
    else:
        
        return game_info

############################################## HTML rosters ##############################################

## [x]Refactored
def scrape_html_rosters(game_ids, disable_print = False, session = None, nested = False):
    
    '''

    --------- Info ---------

    Scrapes the rosters from the HTML endpoint for a given game ID or list of game IDs. Returns a dataframe.
    Can be used standalone but is typically nested within other scraping functions.
    Typically scrapes 6-10 games per second.

    --------- Parameters ---------

    game_ids: a single API game ID (e.g., 2021020001) or list of game IDs

    disable_print: boolean; default: False
        If True, prints progress to the console

    session: requests Session object; default = None
        When using in another scrape function, can pass the requests session to improve speed
    '''
    
    
    game_ids = convert_to_list(obj = game_ids, object_type = 'game ID')
    
    if session == None:
    
        s = s_session()
        
    else:
        
        s = session
        
    if len(game_ids) == 1:
        
        disable_print = True
    
    game_list = []
    
    pbar = tqdm(game_ids, disable = disable_print)
    
    for game_id in pbar:
        
        html_season_id, html_game_id = convert_ids(game_id)
    
        url = f'http://www.nhl.com/scores/htmlreports/{html_season_id}/RO0{html_game_id}.HTM'
        
        page = s.get(url)
        
        page_status = page.status_code
        
        if page_status == 404:

            pbar.set_description(f'{game_id} not available')

            now = datetime.now()

            current_time = now.strftime("%H:%M:%S")

            postfix_str = f'{current_time}'

            pbar.set_postfix_str(postfix_str)
            
            continue
        
        td_dict = {'align':'center', 'class':['teamHeading + border', 'teamHeading + border '], 'width':'50%'}
            
        table_dict = {'align':'center', 'border':'0', 'cellpadding':'0', 'cellspacing':'0', 'width':'100%', 'xmlns:ext':''}
        
        soup = BeautifulSoup(page.content.decode('ISO-8859-1'), 'lxml', multi_valued_attributes = None)

        td_dict = {'align':'center', 'class':['teamHeading + border', 'teamHeading + border '], 'width':'50%'}

        teamsoup = soup.find_all('td', td_dict)

        table_dict = {'align':'center', 'border':'0', 'cellpadding':'0', 'cellspacing':'0', 'width':'100%', 'xmlns:ext':''}

        team_names = {}

        team_soup_list = []

        team_list = ['away', 'home']

        html_roster_list = []

        for idx, team in enumerate(team_list):

            team_names.update({team : teamsoup[idx].get_text()})

            team_soup_list.append((soup.find_all('table', table_dict))[idx].find_all('td'))

        for idx, team_soup in enumerate(team_soup_list):

            length = int(len(team_soup) / 3)

            df = pd.DataFrame(np.array(team_soup).reshape(length, 3))

            df.columns = df.iloc[0]

            df = df.assign(team = team_list[idx], team_name = team_names.get(team_list[idx]), status = 'active').drop(0)

            html_roster_list.append(df)

        if len(soup.find_all('table', table_dict)) > 2:

            scratch_soups = []

            for idx, team in enumerate(team_list):

                team_scratch = (soup.find_all('table', table_dict))[idx + 2].find_all('td')

                if len(team_scratch) > 1:

                    length = int(len(team_scratch) / 3)

                    df = pd.DataFrame(np.array(team_scratch).reshape(length, 3))

                    df.columns = df.iloc[0]

                    df = df.assign(team = team_list[idx], team_name = team_names.get(team_list[idx]), status = 'scratch').drop(0)

                    html_roster_list.append(df)

                else:

                    html_roster_list.append(pd.DataFrame())

        game_df = pd.concat(html_roster_list, ignore_index = True)

        game_df['game_id'] = game_id

        game_df['season'] = html_season_id
        
        game_df.team_name = game_df.team_name.str.normalize('NFKD') \
                                                    .str.encode('ascii', errors='ignore').str.decode('utf-8').str.upper()
        
        replace_teams = {'MONTRAAL CANADIENS': 'MONTREAL CANADIENS', 'CANADIENS MONTREAL': 'MONTREAL CANADIENS',
                         'PHOENIX COYOTES': 'ARIZONA COYOTES'}
        
        game_df.team_name.replace(replace_teams, regex = False, inplace = True)

        ## Rename columns
        new_cols = {'#' : 'player_jersey',
                    'Pos' : 'player_position',
                    'status' : 'player_status',
                    'Nom/Name' : 'player_name',
                    'Name' : 'player_name'}

        game_df.rename(columns = new_cols, inplace = True)

        if 'player_name' not in list(game_df.columns):

            continue

        if 'player_position' not in list(game_df.columns):

            game_df['player_position'] = np.nan

        ## Full names, then fixing
        #game_df.player_name = game_df.player_name.str.split('(').str[0].str.strip()
        
        game_df.player_name = game_df.player_name.str.replace('\(\s?(.*)\)', '', regex = True).str.strip()
        
        game_df.player_name = game_df.player_name.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.upper()

        # Max Pacioretty doesn't exist in ESPN in 2009-2010, sadly.
        replace_dict = {'AlEXANDRE ' : 'ALEX ',
                        'ALEXANDER ' : 'ALEX ',
                        'CHRISTOPHER ' : 'CHRIS ',
                       }

        for old_name, new_name in replace_dict.items():

            game_df.player_name = game_df.player_name.str.replace(old_name, new_name, regex = False, case = False)

        # List of names and fixed from Evolving Hockey Scraper.
        game_df.player_name = game_df.player_name.map(correct_names_dict).fillna(game_df.player_name)

        player_names = ['player_first_name', 'player_last_name']

        for idx, player_name in enumerate(player_names):

            game_df[player_name] = game_df.player_name.str.split(' ', n = 1).str[idx]

        game_df['api_name'] = game_df.player_first_name + '.' + game_df.player_last_name

        double_names_fix = {'SEBASTIAN.AHO2' : np.logical_and(game_df.api_name == 'SEBASTIAN.AHO', game_df.player_position == 'D'),
                            'COLIN.WHITE2': np.logical_and(game_df.api_name == 'COLIN.WHITE', int(html_season_id) >= 20162017), 
                            'SEAN.COLLINS2' : np.logical_and(game_df.api_name == 'SEAN.COLLINS', game_df.player_position != 'D'),
                            'ALEX.PICARD2' : np.logical_and(game_df.api_name == 'ALEX.PICARD', game_df.player_position != 'D'),
                            'ERIK.GUSTAFSSON2' : np.logical_and(game_df.api_name == 'ERIK.GUSTAFSSON', int(html_season_id) >= 20152016),
                            'MIKKO.LEHTONEN2' : np.logical_and(game_df.api_name == 'MIKKO.LEHTONEN', int(html_season_id) >= 20202021),
                            'NATHAN.SMITH2': np.logical_and(game_df.api_name == 'NATHAN.SMITH', int(html_season_id) >= 20212022),
                            'DANIIL.TARASOV2': np.logical_and(game_df.api_name == 'DANIIL.TARASOV', game_df.player_position == 'G')
                           }

        for fix, condition in double_names_fix.items():

            game_df.api_name = np.where(condition, fix, game_df.api_name)

        game_df.api_name = np.where(game_df.api_name == 'COLIN.', 'COLIN.WHITE2', game_df.api_name)
        
        game_df['team_code'] = game_df.team_name.map(team_codes)
        
        game_df['player_abbr'] = game_df.team_code + game_df.player_jersey.astype(str)

        if str(game_id)[4:6] == '01':

            game_df['session'] = 'PR'

        if str(game_id)[4:6] == '02':

            game_df['session'] = 'R'

        if str(game_id)[4:6] == '03':

            game_df['session'] = 'P'

        game_list.append(game_df)
        
        if game_id == game_ids[-1]:
            
            pbar.set_description(f'Finished scraping roster data')
            
        else:
        
            pbar.set_description(f'Finished scraping {game_id}')
        
        now = datetime.now()

        current_time = now.strftime("%H:%M:%S")

        postfix_str = f'{current_time}'
        
        pbar.set_postfix_str(postfix_str)
        
    df = pd.concat(game_list, ignore_index = True)
    
    columns = ['season', 'session', 'game_id', 'team_name', 'team', 'team_code', 'player_name', 'api_name',
               'player_abbr', 'player_jersey', 'player_position', 'player_status']
    
    columns = [x for x in columns if x in df]
    
    df = df[columns]

    if nested == False:

        s.close()
        
    return df

############################################## HTML shifts ##############################################

## []Refactored
def scrape_html_shifts(game_ids, disable_print = False, roster_data = None, game_data = None, session = None, pbp = False, nested = False):
    
    '''

    --------- Info ---------

    Scrapes the shifts from the HTML endpoint for a given game or game IDs. Returns a dictionary with the keys: 'shifts' & 'changes'
    Each dictionary value is a dataframe. 
    Typically takes 1-2 seconds per game

    Can be used standalone but is typically nested within other scraping functions.

    --------- Parameters ---------

    game_ids: a single API game ID (e.g., 2021020001) or list of game IDs

    disable_print: boolean; default: False
        If False, prints progress to the console

    roster: dataframe; default: None
        Can pass a dataframe of roster information to prevent re-scraping from the same endpoint.
        If None, scrapes roster data from the HTML endpoint

    game_data: dataframe, or dataframe; default: None
        Can pass a dataframe of game information to prevent re-scraping from the same endpoint.
        If None, scrapes game information data from the API endpoint

    session: requests Session object; default: None
        When using in another scrape function, can pass the requests session to improve speed

    pbp: boolean; default: False
        If True, additional columns are added for use in the pbp scraper
    '''
    
    ## TO DO:
    ## 1. Add comments and doc string
    ## 2. Ensure columns returned make sense and are in the correct order
    ## 3. Move functionality from pbp scrape to here, but have as a return option
    ## 4. Add season column
    ## 5. TDH had some goalie thing in there that was fucking up my shifts? I think it was only meant for live games?
    
    ## Convert game IDs to list if given a single game ID
    game_ids = convert_to_list(obj = game_ids, object_type = 'game ID')
    
    number_of_games = len(game_ids)
    
    ## Important lists
    shifts_concat = []
    changes_concat = []
    
    scrape_dict = {}
    
    if session == None:
    
        s = s_session()
        
    else:
        
        s = session
        
    pbar = tqdm(game_ids, disable = disable_print)
    
    for game_id in pbar:
        
        if game_data is None:
            
            game_info = scrape_game_info(game_id, disable_print = True, session = s, nested = True)
            
        else:
            
            game_info = game_data[game_data.game_id == game_id].copy(deep = True)

        GAME_SESSION = game_info.game_type.iloc[0]
            
        if roster_data is None:
            
            roster = scrape_html_rosters(game_id, disable_print = True, session = s, nested = True)
            
        else:
            
            roster = roster_data[roster_data.game_id == game_id].copy(deep = True)
            
        roster = roster[roster.player_status != 'scratch'].copy(deep = True)
            
        api_names_dict = dict(zip(roster.player_abbr, roster.api_name))

        positions_dict = dict(zip(roster.player_abbr, roster.player_position))

        html_season_id, html_game_id = convert_ids(game_id)

        urls_dict = {'home': f'http://www.nhl.com/scores/htmlreports/{html_season_id}/TH0{html_game_id}.HTM',
                     'away': f'http://www.nhl.com/scores/htmlreports/{html_season_id}/TV0{html_game_id}.HTM'}

        concat_list = []

        for team, url in urls_dict.items():

            response = requests.get(url)

            soup = BeautifulSoup(response.content.decode('ISO-8859-1'), 'lxml', multi_valued_attributes = None)

            team_name = soup.find('td', {'align':'center', 'class':'teamHeading + border'})

            team_name = unidecode.unidecode(team_name.get_text())

            players = soup.find_all('td', {'class':['playerHeading + border', 'lborder + bborder']})

            players_dict = {}

            for player in players:

                data = player.get_text()

                if ', ' in data:

                    name = data.split(',', 1)

                    number = name[0].split(' ')[0].strip()

                    last_name = name[0].split(' ', 1)[1].strip()

                    first_name = re.sub('\(\s?(.+)\)', '', name[1]).strip()

                    full_name = f'{first_name} {last_name}'
                    
                    if full_name == ' ': 
                        
                        continue

                    players_dict[full_name] = {}

                    players_dict[full_name]['number'] = number

                    players_dict[full_name]['name'] = full_name

                    players_dict[full_name]['shifts'] = []
                    
                else:
                    
                    if full_name == ' ': 
                        
                        continue

                    players_dict[full_name]['shifts'].extend([data])

            for player, shifts in players_dict.items():

                length = int(len(np.array(shifts['shifts'])) / 5)

                for number, shift in enumerate(np.array(shifts['shifts']).reshape(length, 5)):

                    column_names = ['shift_number', 'period', 'shift_start', 'shift_end', 'duration']

                    shift_dict = dict(zip(column_names, shift.flatten()))

                    shift_dict['shift_number'] = int(shift_dict['shift_number'])

                    shift_dict['team'] = team_name

                    conds_dict = {'CANADIENS' in shift_dict['team']: 'MONTREAL CANADIENS',
                                  shift_dict['team'] == 'PHOENIX COYOTES': 'ARIZONA COYOTES'}

                    for condition, value in conds_dict.items():

                        if condition:

                            shift_dict['team'] = value

                    shift_dict['team_code'] = team_codes[shift_dict['team']]

                    shift_dict['venue'] = team

                    shift_dict['player_name'] = shifts['name']

                    shift_dict['player_jersey'] = int(shifts['number'])

                    shift_dict['player_abbr'] = shift_dict['team_code'] + str(shift_dict['player_jersey'])

                    cols = ['shift_start', 'shift_end']

                    for col in cols:

                        shift_dict[col] = unidecode.unidecode(shift_dict[col]).strip()

                        new_col = f"{col.split('_')[1]}_time"

                        shift_dict[new_col] = shift_dict[col].split('/', 1)[0]

                    cols = ['start_time', 'end_time', 'duration']

                    for col in cols:

                        time_split = shift_dict[col].split(':', 1)
                        
                        try:

                            shift_dict[f'{col}_seconds'] = 60 * int(time_split[0]) + int(time_split[1])
                            
                        except ValueError:
                            
                            continue

                    if shift_dict['end_time'] == ' ' or shift_dict['end_time'] == '':

                        shift_dict['end_time_seconds'] = shift_dict['start_time_seconds'] + shift_dict['duration_seconds']

                        shift_dict['end_time'] = str(timedelta(seconds=shift_dict['end_time_seconds'])).split(':', 1)[1]

                    conds_dict = {'OT': 4, 'SO': 5}

                    for old_period, new_period in conds_dict.items():

                        if shift_dict['period'] == old_period:

                            shift_dict['period'] = new_period

                    shift_dict['period'] = int(shift_dict['period'])

                    replace_dict = {'ALEXANDRE ': 'ALEX ', 'ALEXANDER ': 'ALEX ', 'CHRISTOPHER ': 'CHRIS ', 'DE LEO': 'DELEO'}

                    for old_name, new_name in replace_dict.items():

                        shift_dict['player_name'] = shift_dict['player_name'].replace(old_name, new_name)

                    shift_dict['player_name'] = correct_names_dict.get(shift_dict['player_name'], shift_dict['player_name'])

                    #shifts_df.player_name = shifts_df.player_name.map(correct_names_dict).fillna(shifts_df.player_name)

                    shift_dict['api_name'] = api_names_dict.get(shift_dict['player_abbr'])

                    shift_dict['position'] = positions_dict.get(shift_dict['player_abbr'])

                    shift_dict['goalie'] = np.where(shift_dict['position'] == 'G', 1, 0)

                    concat_list.append(shift_dict)
                    
        if concat_list == []:
            
            continue

        shifts_df = pd.DataFrame(concat_list)
        
        periods = list(shifts_df.period.unique())
        
        teams = list(shifts_df.team.unique())
        
        for period in periods:
            
            for team in teams:
                
                mask = np.logical_and(shifts_df.team == team, shifts_df.period == period)
                
                if shifts_df[mask].goalie.sum() < 1:
                    
                    goalie_mask = np.logical_and(shifts_df.team == team, shifts_df.goalie == 1)
                    
                    goalies = shifts_df[goalie_mask].copy(deep = True)
                    
                    if period == 1:
                        
                        goalie = dict(goalies.iloc[0])
                        
                        goalie = pd.DataFrame(goalie, index = [0])
                        
                    else:
                        
                        goalie = goalies[goalies.period == period - 1].copy(deep = True)
                        
                    if period < 4:

                        goalie.start_time = '0:00'

                        goalie.end_time = '20:00'

                        goalie.duration = '20:00'

                        goalie.shift_start = '0:00 / 20:00'

                        goalie.shift_end = '20:00 / 0:00'
                        
                    else:
                        
                        goalie.start_time = '0:00'
                        
                        total_seconds = 300
                        
                        if str(game_id)[4:6] == '03':
                            
                            goalie.shift_start = '0:00 / 20:00'
                            
                            total_seconds = 1200
                            
                        goalie.shift_start = '0:00 / 5:00'
                            
                        end_time_seconds = shifts_df[shifts_df.period == period].end_time_seconds.max()
                        
                        end_time_minutes = str(timedelta(seconds=end_time_seconds)).split(':', 1)[1]
                        
                        remainder_minutes = str(timedelta(seconds=(total_seconds - end_time_seconds))).split(':', 1)[1]
                        
                        goalie.end_time = end_time_minutes
                        
                        goalie.shift_end = f'{end_time_minutes} / {remainder_minutes}'
        
                    goalie.period = period
                            
                    shifts_df = pd.concat([shifts_df, goalie], ignore_index = True)
                    
                    shifts_df.shift_number = np.where(shifts_df.goalie == 1,
                                                      shifts_df.groupby('player_abbr').cumcount() + 1,
                                                      shifts_df.shift_number)
            
            if period < 4:
                
                end_time_long = '20:00 / 0:00'
                
                end_time_minutes = '20:00'
                
                end_time_seconds = 1200
                
            else:
                
                total_seconds = 300
                        
                if str(game_id)[4:6] == '03':

                    total_seconds = 1200
                
                end_time_seconds = shifts_df[shifts_df.period == period].end_time_seconds.max()

                end_time_minutes = str(timedelta(seconds=end_time_seconds)).split(':', 1)[1]

                remainder_minutes = str(timedelta(seconds=(total_seconds - end_time_seconds))).split(':', 1)[1]

                end_time_long = f'{end_time_minutes} / {remainder_minutes}'
                
            goalie_cond = np.logical_and(shifts_df.goalie == 1, shifts_df.period == period)

            conditions = np.logical_and(goalie_cond, shifts_df.shift_end == '0:00 / 0:00')

            shifts_df.shift_end = np.where(conditions, end_time_long, shifts_df.shift_end)
            
            shifts_df.end_time = np.where(conditions, end_time_minutes, shifts_df.end_time)
            
            shifts_df.end_time_seconds = np.where(conditions, end_time_seconds, shifts_df.end_time_seconds)

        columns = ['start_time', 'end_time', 'duration']

        for column in columns: 

            time_split = shifts_df[column].astype(str).str.split(':')
            
            shifts_df[f'{column}_seconds'] = 60 * time_split.str[0].astype(int) + time_split.str[1].astype(int)
            
        cond = shifts_df.start_time_seconds > shifts_df.end_time_seconds
        
        shifts_df.end_time = np.where(cond, '20:00', shifts_df.end_time)
        
        shifts_df.end_time_seconds = np.where(cond, 1200, shifts_df.end_time_seconds)
        
        shifts_df.shift_end = np.where(cond, '20:00 / 0:00', shifts_df.shift_end)
        
        shifts_df.duration_seconds = shifts_df.end_time_seconds - shifts_df.start_time_seconds
        
        shifts_df.duration = pd.to_datetime(shifts_df.duration_seconds, unit = 's').dt.strftime('%M:%S')
        
        shifts_df = shifts_df.copy().sort_values(by = ['venue', 'player_name', 'shift_number']).reset_index(drop = True)

        group_dict = {'on': ['team', 'team_code', 'period', 'start_time', 'start_time_seconds'], 
                      'off': ['team', 'team_code', 'period', 'end_time', 'end_time_seconds']}

        times_dict = {'on': {'start_time': 'time'}, 
                      'off': {'end_time': 'time'}}

        changes_dict = {}

        for change_type, group_list in group_dict.items():

            column_names = times_dict[change_type]

            sort_list = ['team', 'period', 'time']

            if change_type == 'on':

                df = shifts_df.groupby(group_list, as_index = False)\
                        .agg(players_on = ('player_name', tuple),
                             players_on_abbr = ('player_abbr', tuple),
                             players_on_api = ('api_name', tuple),
                             positions_on = ('position', tuple),
                             number_on = ('player_name', 'count'))\
                        .rename(columns = column_names)\
                        .sort_values(by = sort_list)

            if change_type == 'off':

                df = shifts_df.groupby(group_list, as_index = False)\
                        .agg(players_off = ('player_name', tuple),
                             players_off_abbr = ('player_abbr', tuple),
                             players_off_api = ('api_name', tuple),
                             positions_off = ('position', tuple),
                             number_off = ('player_name', 'count'))\
                        .rename(columns = column_names)\
                        .sort_values(by = sort_list)

            changes_dict.update({change_type: df.reset_index(drop = True)})

        merge_list = ['team', 'team_code', 'period', 'time']

        changes_on = changes_dict['on'].merge(changes_dict['off'], on = merge_list, how = 'left')
        changes_off = changes_dict['off'].merge(changes_dict['on'], on = merge_list, how = 'left')

        changes_df = pd.concat([changes_on, changes_off], ignore_index = True)\
                            .sort_values(by = ['period', 'start_time_seconds'], ascending = True)\
                            .drop_duplicates().reset_index(drop = True)
        
        changes_df.start_time_seconds = changes_df.start_time_seconds.fillna(1200)
        
        changes_df.end_time_seconds = changes_df.end_time_seconds.fillna(0)
        
        changes_df = changes_df.sort_values(by = ['period', 'start_time_seconds'], ascending = True).reset_index(drop = True)

        time_split = changes_df.time.astype(str).str.split(':')

        changes_df['period_seconds'] = 60 * time_split.str[0].astype(int) + time_split.str[1].astype(int)

        changes_df['game_seconds'] = (changes_df.period - 1) * 1200 + changes_df.period_seconds
        
        shifts_df['game_id'] = game_id
        shifts_df['season'] = str(game_id)[:4] + str(int(str(game_id)[:4]) + 1)
        
        changes_df['game_id'] = game_id
        changes_df['season'] = str(game_id)[:4] + str(int(str(game_id)[:4]) + 1)

        changes_df['session'] = GAME_SESSION
        shifts_df['session'] = GAME_SESSION

        group_list = ['season', 'session', 'game_id', 'team', 'team_code'] 

        changes_df['team_shift_idx'] = changes_df.groupby(group_list).transform('cumcount') + 1
        
        #if pbp == True:

        changes_df['event'] = 'CHANGE'

        changes_df['description'] = 'Players on: ' + changes_df.players_on.str.join(', ') + \
                                            ' / Players off: ' + changes_df.players_off.str.join(', ')

        #team_type_dict = dict(zip(game_info.status, game_info.team_tri_code))

        #columns = ['event_team', 'home_team_abbr', 'away_team_abbr']

        #changes_df['event_team'] = changes_df.venue.map(team_type_dict)

        #changes_df['home_team_abbreviated'] = np.where(changes_df.venue == 'home', team_type_dict['home'], team_type_dict['away'])

        #changes_df['away_team_abbreviated'] = np.where(changes_df.venue == 'away', team_type_dict['away'], team_type_dict['home'])

        #team_name_df = changes_df[['event_team', 'team']].drop_duplicates()

        #team_name_dict = dict(zip(team_name_df.event_team, team_name_df.team))

        #team_types = ['home', 'away']

        #for team in team_types:

        #    changes_df[team + '_team'] = changes_df[team + '_team_abbreviated'].map(team_name_dict)
        
        shifts_concat.append(shifts_df)
        
        changes_concat.append(changes_df)
        
        if game_id == game_ids[-1]:
            
            pbar.set_description(f'Finished scraping shifts data')
            
        else:
        
            pbar.set_description(f'Finished scraping {game_id}')
        
        now = datetime.now()

        current_time = now.strftime("%H:%M:%S")

        postfix_str = f'{current_time}'
        
        pbar.set_postfix_str(postfix_str)
        
    if shifts_concat == []:
        
        shifts_df = pd.DataFrame()
        
    else:
        
        shifts_df = pd.concat(shifts_concat, ignore_index = True)
        
        columns = ['season', 'session', 'game_id', 'team', 'team_code', 'venue', 'player_name', 'player_jersey', 'player_abbr',
                   'shift_number', 'period', 'shift_start', 'shift_end', 'duration', 'start_time', 'end_time',
                   'start_time_seconds', 'end_time_seconds', 'duration_seconds', 'api_name', 'goalie', 'position']
        
        columns = [x for x in columns if x in shifts_df.columns]
                   
        shifts_df = shifts_df[columns]
        
    if changes_concat == []:
        
        changes_df = pd.DataFrame()
        
    else:
        
        changes_df = pd.concat(changes_concat, ignore_index = True)
                   
        columns = ['season', 'session', 'game_id', 'team', 'team_code', 'team_shift_idx', 'period', 'time',
                    'period_seconds',  'game_seconds', 'number_on',
                   'players_on', 'players_on_api', 'positions_on', 'number_off', 'players_off', 'players_off_api', 
                   'positions_off', 'players_on_abbr', 'players_off_abbr', 'event', 'description', 'event_team',
                   'home_team_abbreviated', 'away_team_abbreviated']
        
        columns = [x for x in columns if x in changes_df.columns]
        
        changes_df = changes_df[columns]

    if nested == False:

        s.close()
        
    scrape_dict.update({'shifts': shifts_df, 
                       'changes': changes_df})
    
    return scrape_dict

############################################## API events ##############################################

## []Refactored
def scrape_api_events(game_ids, live_response = None, session = None, disable_print = False):
    '''
    Scrapes the event data from the API endpoint. Returns a dataframe. Data do not exist before 2010-2011 season

    Used within the main play-by-play scraper
    Parameters:
    game_ids: a single API game ID (e.g., 2021020001) or list of game IDs

    response_data: JSON object; default: None
        When using in another scrape function, can pass the live endpoint response as a JSON object to prevent redundant hits

    disable_print: boolean; default: False
        If False, prints progress to the console

    session: requests Session object; default = None
        When using in another scrape function, can pass the requests session to improve speed

    '''
    
    ## TO DO:
    ## 1. Flip blocked shots
    ## 2. Penalty players
    ## 3. add comments and edit doc string
    ## 4. double check columns are in the right order
    
    ## Convert game IDs to list if given a single game ID
    game_ids = convert_to_list(obj = game_ids, object_type = 'game ID')
    
    ## Important lists
    bad_list = []
    concat_list = []
    
    if live_response == None:
        
        if session == None:
        
            s = s_session()
        
        else:
            
            s = session
    
    pbar = tqdm(game_ids, disable = disable_print)
    
    for game_id in pbar:
        
        if str(game_id).isdigit() == False or len(str(game_id)) != 10 or int(str(game_id)[:4]) < 2010 or str(game_id)[4:6] not in ['02', '03']:
            
            pbar.set_description(f'{game_id} not a valid game_id')

            now = datetime.now()

            current_time = now.strftime("%H:%M:%S")

            postfix_str = f'{current_time}'

            pbar.set_postfix_str(postfix_str)
            
            bad_list.append(game_id)
            
            continue
            
        if live_response == None:
        
            response = s.get(f'https://statsapi.web.nhl.com/api/v1/game/{game_id}/feed/live').json()
            
        else:
            
            response = live_response
            
        season = str(game_id)[:4] + str(int(str(game_id)[:4]) + 1)
        
        conds_dict = {str(game_id)[4:6] == '01': 'PR', str(game_id)[4:6] == '02': 'R', str(game_id)[4:6] == '03': 'P'}
            
        for condition, value in conds_dict.items():

            if condition:

                session = value
        
        teams = response['gameData']['teams']
        
        teams_info = {}
        
        for team, info in teams.items():
                
            teams_info[f'{team}_team'] = info['triCode']

            teams_info[f'{team}_team_name'] = info['name'].upper()

            conditions = ['CANADIENS' in teams_info[f'{team}_team_name'], teams_info[f'{team}_team_name'] == 'PHOENIX COYOTES']

            values = ['MONTREAL CANADIENS', 'ARIZONA COYOTES']

            for condition, value in zip(conditions, values):

                if condition:

                    teams_info[f'{team}_team_name'] = value

            if teams_info[f'{team}_team'] == 'PHX':

                teams_info[f'{team}_team'] = 'ARI'
        
        start_dt = pd.to_datetime(response['gameData']['datetime']['dateTime'])
        
        start_date = start_dt.tz_convert('US/Eastern').strftime('%Y-%m-%d')
        
        start_time = start_dt.tz_convert('US/Eastern').strftime('%H:%M')
        
        plays = response['liveData']['plays']['allPlays']
        
        if plays == []:
            
            pbar.set_description(f'{game_id} not available')

            now = datetime.now()

            current_time = now.strftime("%H:%M:%S")

            postfix_str = f'{current_time}'

            pbar.set_postfix_str(postfix_str)
            
            bad_list.append(game_id)
            
            continue
            
        for play in plays:
            
            result = play['result']
            
            about = play['about']
            
            coordinates = play['coordinates']
            
            play['season'] = season
            
            play['game_id'] = int(game_id)
            
            play['session'] = session
            
            play['start_date'] = start_date
            
            play['start_time'] = start_time
            
            play['date_time'] = about['dateTime']
            
            for key, value in teams_info.items():
                
                play[key] = value
            
            if 'players' in play.keys():
                
                players = play['players']
                
                for idx, player in enumerate(players):
                    
                    num = idx + 1
                    
                    play[f'player_{num}_type'] = player['playerType'].upper()
                    
                    player = player['player']
                    
                    play[f'player_{num}'] = unidecode.unidecode(player['fullName'].upper())
                    
                    replace_dict = {'ALEXANDRE ': 'ALEX ', 'ALEXANDER ': 'ALEX ', 'CHRISTOPHER ': 'CHRIS '}
                    
                    for old_name, new_name in replace_dict.items():
                        
                        play[f'player_{num}'] = play[f'player_{num}'].replace(old_name, new_name)
                    
                    play[f'player_{num}'] = correct_names_dict.get(play[f'player_{num}'], play[f'player_{num}'])
                    
                    play[f'player_{num}_id'] = player['id']
                    
                    name_split = play[f'player_{num}'].split(' ', 1)
                    
                    play[f'player_{num}_api'] = name_split[0].strip() + '.' + name_split[1].strip()
                    
                    play[f'player_{num}_api'] = correct_api_names_dict.get(play[f'player_{num}_id'], play[f'player_{num}_api'])
                    
                del play['players']
                    
            play['event_idx'] = int(about['eventIdx'])
            
            play['period'] = int(about['period'])
            
            play['period_type'] = about['periodType']
            
            play['period_time'] = about['periodTime']
            
            play['period_time_remaining'] = about['periodTimeRemaining']
            
            keys = ['period_time', 'period_time_remaining']
            
            for key in keys:
                
                time_split = play[key].split(':', 1)
                
                new_key = key.replace('time', 'seconds')
            
                play[new_key] = int(time_split[0]) * 60 + int(time_split[1])
                
            if play['period_type'] != 'SHOOTOUT':
                
                play['game_seconds'] = (int(play['period']) - 1) * 1200 + play['period_seconds']
                
            else:
                
                play['game_seconds'] = 3900 + play['period_seconds']
            
            play['event'] = result['eventTypeId']
            
            event_dict = {'BLOCKED_SHOT': 'BLOCK', 'BLOCKEDSHOT': 'BLOCK', 'MISSED_SHOT': 'MISS', 'FACEOFF': 'FAC',
                          'PENALTY': 'PENL', 'GIVEAWAY': 'GIVE', 'TAKEAWAY': 'TAKE', 'MISSEDSHOT': 'MISS', 
                          'PERIOD_START': 'PSTR', 'PERIOD_END': 'PEND', 'PERIOD_OFFICIAL': 'POFF', 'GAME_OFFICIAL': 'GOFF',
                          'GAME_SCHEDULED': 'GSCH', 'GAME_END': 'GEND', 'CHALLENGE': 'CHL', 'SHOOTOUT_COMPLETE': 'SOC', 
                          'EARLY_INT_START': 'EISTR', 'EARLY_INT_END': 'EIEND', 'PERIOD_READY': 'PREADY'}
            
            play['event'] = event_dict.get(play['event'], play['event'])
            
            play['event_type'] = result['event'].upper()
            
            play['event_description'] = result['description']
            
            play['event_code'] = result['eventCode']
            
            for key, value in coordinates.items():
                    
                    play[f'coords_{key}'] = value
                    
            for key, value in about['goals'].items():
                
                play[f'{key}_score'] = value
                
            if 'team' in play.keys():
                
                for key, value in play['team'].items():
                    
                    new_key = f'event_team_{key}'
                    
                    if key == 'triCode':
                        
                        new_key = 'event_team'
                
                    play[new_key] = value
                    
                play['event_team_name'] = play['event_team_name'].upper()
                
                conditions = ['CANADIENS' in play['event_team_name'], play['event_team_name'] == 'PHOENIX COYOTES']
                
                values = ['MONTREAL CANADIENS', 'ARIZONA COYOTES']
                
                for condition, value in zip(conditions, values):
                    
                    if condition:
                        
                        play['event_team_name'] = value
                        
                if play['event_team'] == 'PHX':
                    
                    play['event_team'] = 'ARI'
                
                del play['team']
                
            else:
                    
                team_keys = ['event_team_id', 'event_team_name', 'event_team_link', 'event_team_code']
                    
                for key in team_keys:

                    play[key] = None
                    
            ## Swapping the players for blocked shots
            
            if play['event'] == 'BLOCK' and play['player_1_type'] == 'BLOCKER':
                
                play['player_1'], play['player_2'] = play['player_2'], play['player_1']
                play['player_1_id'], play['player_2_id'] = play['player_2_id'], play['player_1_id']
                play['player_1_api'], play['player_2_api'] = play['player_2_api'], play['player_1_api']
                play['player_1_type'], play['player_2_type'] = play['player_2_type'], play['player_1_type']
                
                if play['event_team'] == play['home_team']:
                    
                    play['event_team'] = play['away_team']
                    
                elif play['event_team'] == play['away_team']:
                    
                    play['event_team'] = play['home_team']
                    
            keys_dict = {'secondaryType': 'event_detail_api', 'strength_code': 'strength_code',
                         'strength_name': 'strength_name', 'gameWinningGoal': 'game_winning_goal',
                         'emptyNet': 'empty_net_goal', 'penaltySeverity': 'penalty_severity',
                         'penaltyMinutes': 'penalty_minutes'}
            
            for old_key, new_key in keys_dict.items():
                
                if old_key in result.keys():
                
                    play[new_key] = result[old_key]
                    
            if play['event'] == 'PENL' and 'too many men' in play['event_description'].lower():
                
                cols = {'player_1': 'player_2', 'player_1_id': 'player_2_id',
                        'player_1_api': 'player_2_api'}
                
                for old, new in cols.items():
                    
                    play[new] = play[old]
                    
                    play[old] = 'BENCH'
                    
                play['player_2_type'] = 'SERVED BY'
                
            if play['event'] == 'PSTR' and play['period'] == 1:
                
                game_start = pd.to_datetime(play['date_time'])
                    
            del play['result']
            
            del play['about']
            
            del play['coordinates']
        
        for play in plays:
            
            time_elapsed = pd.to_datetime(play['date_time']) - game_start
            
            play['time_elapsed'] = str(time_elapsed)
            
            play['time_elapsed_seconds'] = time_elapsed.total_seconds()
            
            play['game_start'] = game_start.tz_convert('US/Eastern').strftime('%H:%M')
            
        #plays = pd.DataFrame(plays)
        
        ## Creating opposition team columns, after swapping out the teams in blocked shots
        
        #columns = ['team_id', 'team_name', 'team_link', 'team']
        
        #for column in columns:
            
        #    teams_dict = {}
            
        #    teams_list = plays['event_' + column].unique()
            
        #    teams_dict.update({teams_list[1]: teams_list[2],
        #                       teams_list[2]: teams_list[1]})
            
        #    plays['event_' + column] = np.where(plays.event_type == 'BLOCK',
        #                                        plays['event_' + column].map(teams_dict).fillna(plays['event_' + column]),
        #                                        plays['event_' + column])
            
        #    plays['opp_' + column] = plays['event_' + column].map(teams_dict)
        
        ## Swapping out event players 1 and 2 for blocked shots
        #players_dict = {}
        
        #players_dict.update({'player_1': 'player_2',
        #                     'player_2': 'player_1'})
        
        #columns = ['link', 'type', 'id', 'api']
        
        #for column in columns:
            
        #    players_dict.update({'player_1_' + column: 'player_2_' + column,
        #                         'player_2_' + column: 'player_1_' + column})
            
        #mask = plays.event_type == 'BLOCK'
        
        #plays.update(plays.loc[mask].rename(players_dict, axis = 1))
            
        ## Finishing up

        concat_list.extend(plays)
        
        if game_id == game_ids[-1]:
            
            events_df = pd.DataFrame(concat_list)#, ignore_index = True)
            
            group_list = ['game_id', 'period', 'game_seconds', 'event_team', 'event', 'player_1_id']
            
            events_df['version'] = events_df[~pd.isna(events_df.player_1_id)].groupby(group_list).transform('cumcount') + 1
            
            events_df.version = events_df.version.fillna(1)
    
            columns = ['season', 'game_id', 'session', 'start_date', 'start_time', 'game_start', 'home_team', 'home_team_name',
                       'away_team', 'away_team_name','event_idx', 'period', 'period_type', 'period_time',
                       'period_time_remaining', 'period_seconds', 'period_seconds_remaining', 'game_seconds',
                       'event_type', 'event', 'event_description', 'coords_x', 'coords_y', 'event_code', 'date_time', 'home_score',
                       'away_score', 'event_team', 'event_team_name', 'event_team_id', 'event_team_link', 'player_1', 
                       'player_1_id', 'player_1_api', 'player_1_type', 'player_2', 'player_2_id', 'player_2_api',
                       'player_2_type', 'player_3', 'player_3_id', 'player_3_api', 'player_3_type', 'player_4',
                       'player_4_id', 'player_4_api', 'player_4_type', 'game_winning_goal', 'empty_net_goal',
                       'penalty_severity', 'penalty_minutes', 'time_elapsed', 'time_elapsed_seconds', 'version']

            columns = [x for x in columns if x in events_df.columns]

            events_df = events_df[columns]
            
            pbar.set_description(f'Finished scraping events data from the NHL API')
            
        else:
        
            pbar.set_description(f'Finished scraping {game_id}')
        
        now = datetime.now()

        current_time = now.strftime("%H:%M:%S")

        postfix_str = f'{current_time}'
        
        pbar.set_postfix_str(postfix_str)
        
    return events_df
    
############################################## HTML events ##############################################

## []Refactored
def scrape_html_events(game_ids, disable_print = False, game_data = None, roster_data = None, session = None):
    '''
    Scrapes the event data from the HTML endpoint. Returns a dataframe.

    Used within the main play-by-play scraper
    Parameters:
    game_ids: a single API game ID (e.g., 2021020001) or list of game IDs

    disable_print: boolean; default: False
        If False, prints progress to the console

    roster_data: dataframe; default: None
        Can pass a dataframe of roster information to prevent re-scraping from the same endpoint.
        If empty, scrapes roster data from the HTML endpoint
    
    game_data: dataframe; default: None
        Can pass a dataframe of game information to prevent re-scraping from the same endpoint.
        If empty, scrapes game information data from the API endpoint

    session: requests Session object; default = None
        When using in another scrape function, can pass the requests session to improve speed
    '''    
    
    ## TO DO:
    ## 1. Flip faceoff players so winner is always event_player_1
    ## 2. Penalty players
    
    ## IMPORTANT LISTS AND DICTIONARIES
    NEW_TEAMS_DICT = {'L.A': 'LAK', 'N.J': 'NJD', 'S.J': 'SJS', 'T.B': 'TBL', 'PHX': 'ARI'}
    EVENT_LIST = ['GOAL', 'SHOT', 'MISS', 'BLOCK', 'FAC', 'HIT', 'GIVE', 'TAKE', 'PENL', 'CHANGE']
    FENWICK_EVENTS = ["SHOT", "GOAL", "MISS"]
    CORSI_EVENTS = ["SHOT", "GOAL", "MISS", "BLOCK"]
    #even_strength = ["5v5", "4v4", "3v3"]
    #uneven_strength = ["5v4", "4v5", "5v3", "3v5", "4v3", "3v4", "5vE", "Ev5", "4vE", "Ev4", "3vE", "Ev3"]
    #pp_strength = ["5v4", "4v5", "5v3", "3v5", "4v3", "3v4"]
    #empty_net = ["5vE", "Ev5", "4vE", "Ev4", "3vE", "Ev3"]
    
    ## Convert game IDs to list if given a single game ID
    game_ids = convert_to_list(obj = game_ids, object_type = 'game ID')
    
    ## Important lists
    BIG_LIST = []
    
    if session == None:
    
        s = s_session()
        
    else:
        
        s = session
        
    pbar = tqdm(game_ids, disable = disable_print)

    for game_id in pbar:
        
        if game_data is None:
            
            game_info = scrape_game_info(game_id, disable_print = True, session = s)
            
        else:
            
            game_info = game_data[game_data.game_id == game_id]
            
        if roster_data is None:
            
            roster = scrape_html_rosters(game_id, disable_print = True, session = s)
            
        else:
            
            roster = roster_data[roster_data.game_id == game_id]
            
        season = str(game_id)[:4] + str(int(str(game_id)[:4]) + 1)
        
        conds_dict = {str(game_id)[4:6] == '01': 'PR', str(game_id)[4:6] == '02': 'R', str(game_id)[4:6] == '03': 'P'}
            
        for condition, value in conds_dict.items():

            if condition:

                session = value

        ## THE BELOW ARE IMPORTANT DICTIONARIES AND LISTS OF INFORMATION COLLECTED FROM THE GAME INFO AND ROSTERS ENDPOINTS
        
        ## Dictionary with the full team name as keys with 'home' or 'away' as values
        TEAMS_DICT = dict(zip(game_info.team_name, game_info.status)) 

        ## Dictionary with 'home' or 'away' as keys as the full team name as values
        TEAMS_DICT_REV = dict(zip(game_info.status, game_info.team_name)) 

        ## Dictionary with the full team name as keys with 'home' or 'away' as values
        TEAMS_DICT_SHORT = dict(zip(game_info.team_tri_code, game_info.status))
        
        ## Dictionary with 'home' or 'away' as keys as the full team name as values
        TEAMS_DICT_SHORT_REV = dict(zip(game_info.status, game_info.team_tri_code))

        ## Team names
        HOME_TEAM_NAME = TEAMS_DICT_REV['home']
        HOME_TEAM_NAME_SHORT = TEAMS_DICT_SHORT_REV['home']

        AWAY_TEAM_NAME = TEAMS_DICT_REV['away']
        AWAY_TEAM_NAME_SHORT = TEAMS_DICT_SHORT_REV['away']
        
        ## Roster stuff
        
        roster = roster[roster.player_status != 'scratch']
        
        PLAYER_NAMES = dict(zip(roster.player_abbr, roster.player_name))
        PLAYER_API_NAMES = dict(zip(roster.player_abbr, roster.api_name))

        ## Game type, either regular season or playoffs
        GAME_SESSION = game_info.game_type.iloc[0]
            
        html_season_id, html_game_id = convert_ids(game_id)

        url = f'http://www.nhl.com/scores/htmlreports/{html_season_id}/PL0{html_game_id}.HTM'

        response = s.get(url)

        soup = BeautifulSoup(response.content.decode('ISO-8859-1'), 'lxml')
        
        events = []

        if soup.find('html') is None:

            pbar.set_description(f'{game_id} not a valid game_id')

            now = datetime.now()

            current_time = now.strftime("%H:%M:%S")

            postfix_str = f'{current_time}'

            pbar.set_postfix_str(postfix_str)
            
            bad_list.append(game_id)
            
            continue 

        tds = soup.find_all("td", {"class": re.compile('.*bborder.*')})

        events_data = hs_strip_html(tds)
        
        events_data = [unidecode.unidecode(x).replace('\n ', ', ').replace('\n', '') for x in events_data]
        
        length = int(len(events_data) / 8)
        
        events_data = np.array(events_data).reshape(length, 8)

        for idx, event in enumerate(events_data):
            
            column_names = ['event_idx', 'period', 'strength', 'time', 'event', 'description', 'away_skaters', 'home_skaters']

            if '#' in event:

                continue

            else:

                event = dict(zip(column_names, event))

                events.append(event)
                
        ## Writing regex expression
        
        event_team_re = re.compile('^([A-Z]{3}|[A-Z]\.[A-Z])')
        numbers_re = re.compile('#([0-9]{1,2})')
        event_players_re = re.compile('([A-Z]{3}\s+\#[0-9]{1,2})')
        positions_re = re.compile('([A-Z]{1,2})')
        skaters_re = re.compile(r'(\d+)')
        zone_re = re.compile(r'([A-Za-z]{3}). Zone')
        penalty_re = re.compile('([A-Za-z]*|[A-Za-z]*-[A-Za-z]*|[A-Za-z]*\s+\(.*\))\s*\(')
        penalty_l_re = re.compile('(\d+) min')
        shot_re = re.compile(',\s+([A-za-z]*|[A-za-z]*-[A-za-z]*),')
        distance_re = re.compile('(\d+) ft')
        served_re = re.compile('([A-Z]{3}).*#([0-9]+)')
        served_drawn_re = re.compile('([A-Z]{3})\s#.*\sServed By: #([0-9]+)')
        
        for event in events:
            
            event['season'] = season
            
            event['game_id'] = game_id
            
            event['session'] = session
            
            for team_type, team_name in TEAMS_DICT_REV.items():

                event[team_type + '_team'] = team_name

                event[team_type + '_team_abbr'] = TEAMS_DICT_SHORT_REV[team_type]
                
            og_time = event['time']

            time_split = event['time'].split(':')

            event['period_time'] = time_split[0].zfill(2) + ':' + time_split[1][:2]
            
            event['period'] = int(event['period'])
            
            event['period_seconds'] = (60 * int(event['period_time'].split(':')[0])) + int(event['period_time'].split(':')[1])
            
            if event['period'] == 5 and GAME_SESSION == 'R':
                
                event['game_seconds'] = 3900 + event['period_seconds']
             
            else:
                
                event['game_seconds'] = (int(event['period']) - 1) * 1200 + event['period_seconds']
                
            for old_name, new_name in NEW_TEAMS_DICT.items():

                event['description'] = event['description'].replace(old_name, new_name)

            if game_id == 2012020018:

                bad_names = {'EDM #9': 'VAN #9', 'VAN #93': 'EDM #93', 'VAN #94': 'EDM #94'}

                for bad_name, good_name in bad_names.items():

                    event['description'] = event['description'].replace(bad_name, good_name)

            if game_id == 2018021133:

                event['description'] = event['description'].replace('WSH TAKEAWAY - #71 CIRELLI', 'TBL TAKEAWAY - #71 CIRELLI')
                
            if event['event'] != 'STOP':
                
                try:
                
                    event['event_team'] = re.search(event_team_re, event['description']).group(1)
                
                except AttributeError:
                    
                    continue
                    
            columns = ['event_team', 'home_team_abbr', 'away_team_abbr']
            
            columns = [x for x in columns if x in event.keys()]

            for col in columns:

                event[col] = NEW_TEAMS_DICT.get(event[col], event[col])
                
            event_list = ['GOAL', 'SHOT', 'TAKE', 'GIVE']
                
            if event['event'] in event_list:
                
                event_players = [event['event_team'] + num for num in re.findall(numbers_re, event['description'])]
                
            else:
                
                event_players = re.findall(event_players_re, event['description'])
                
                if event['event'] == 'FAC' and event['event_team'] == event['home_team_abbr']:
                    
                    event_players.reverse()
            
            for idx, event_player in enumerate(event_players):
                
                num = idx + 1
                
                event[f'event_player_{num}'] = PLAYER_NAMES[event_player.replace(' #', '')]
                
                event[f'event_player_{num}_api'] = PLAYER_API_NAMES[event_player.replace(' #', '')]
                
            teams_l = ['home', 'away']
            
            for team in teams_l:
                
                if event['event'].upper() == 'PSTR':
                    
                    event[f'{team}_skaters'] = np.nan
                    
                    event[f'{team}_positions'] = np.nan
                    
                else:
                
                    event[f'{team}_positions'] = re.findall(positions_re, str(event[f'{team}_skaters']))

                    skaters = re.findall(skaters_re, str(event[f'{team}_skaters']))

                    event[f'{team}_skaters'] = [event[f'{team}_team_abbr'] + str(x) for x in skaters]
            
            try:
                
                event['zone'] = re.search(zone_re, event['description']).group(1).upper()
                
            except AttributeError:
                
                continue
                
            penalty_events = ['PENL', 'DELPEN'] 
                
            if event['event'] in penalty_events:
                
                if 'team' in event['description'].lower() and 'served by' in event['description'].lower():
                    
                    event['event_player_1'] = 'BENCH'
                    
                    event['event_player_1_api'] = 'BENCH'
                    
                    try:
                    
                        served_by = re.search(served_re, event['description'])
                    
                        served_name = served_by.group(1) + str(served_by.group(2))
                        
                        event[f'event_player_2'] = PLAYER_NAMES[served_name]
                
                        event[f'event_player_2_api'] = PLAYER_API_NAMES[served_name]
                        
                    except AttributeError:
                        
                        continue
                        
                if 'served by' in event['description'].lower() and 'drawn by' in event['description'].lower():
                    
                    try:
                    
                        served_by = re.search(served_drawn_re, event['description'])
                    
                        served_name = served_by.group(1) + str(served_by.group(2))
                        
                        event[f'event_player_3'] = PLAYER_NAMES[served_name]
                
                        event[f'event_player_3_api'] = PLAYER_API_NAMES[served_name]
                        
                    except AttributeError:
                        
                        continue
                
                event['penalty'] = re.search(penalty_re, event['description']).group(1).upper()
                
                event['penalty_length'] = int(re.search(penalty_l_re, event['description']).group(1))
                
            shot_events = ['GOAL', 'SHOT', 'MISS', 'BLOCK']
            
            if event['event'] in shot_events:
                
                try:
                
                    event['shot_type'] = re.search(shot_re, event['description']).group(1).upper()
                    
                except AttributeError:
                
                    continue
                
            try:
                
                event['event_distance'] = int(re.search(distance_re, event['description']).group(1))
                
            except AttributeError:
                
                continue
                
        events_df = pd.DataFrame(events)
                
        BIG_LIST.append(events_df)
                
        if game_id == game_ids[-1]:
            
            pbar.set_description(f'Finished scraping events data from the HTML endpoint')
            
            events_df = pd.concat(BIG_LIST, ignore_index = True)
            
            group_list = ['game_id', 'period', 'game_seconds', 'event_team', 'event', 'event_player_1']
            
            events_df['version'] = events_df[~pd.isna(events_df.event_player_1)].groupby(group_list).transform('cumcount') + 1
            
            events_df.version = events_df.version.fillna(1)
            
            columns = ['season', 'game_id', 'session', 'event_idx', 'period', 'period_seconds', 'game_seconds',
                       'strength', 'period_time', 'event',
                       'description', 'away_skaters', 'home_skaters', 'home_team', 'home_team_abbr',
                       'away_team', 'away_team_abbr', 'event_team', 'event_player_1', 'event_player_1_api',
                       'event_player_2', 'event_player_2_api', 'event_player_3', 'event_player_3_api', 'home_positions',
                       'away_positions', 'zone', 'distance', 'shot_type', 'penalty', 'penalty_length', 'version']
            
            columns = [x for x in columns if x in events_df.columns]
            
            events_df = events_df[columns]

        else:

            pbar.set_description(f'Finished scraping {game_id}')

        now = datetime.now()

        current_time = now.strftime("%H:%M:%S")

        postfix_str = f'{current_time}'

        pbar.set_postfix_str(postfix_str)
                
    return events_df





