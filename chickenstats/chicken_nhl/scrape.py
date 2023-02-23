############################################## Introduction ##############################################

# Welcome to the chicken_nhl scraper functions

# All credit to Drew Hynes and his nhlapi project (https://gitlab.com/dword4/nhlapi)

# The two most important functions are: (1) scrape_schedule; and (2) scrape_pbp
# The play-by-play function takes game IDs, which can be sourced using the schedule scraper


############################################## Things to keep track of ##############################################

# 1. Ensure correct data types are returned by each scraper
# 2. QC names against Evolving Hockey to ensure EH IDs match
# 3. QC everything over the years

## Changes
    # 1. Add event types and descriptions
    # 2. Add version columns
    # 3. Ensure dictionary keys are returned in the correct order
    # 4. Change time column to 0:00 format

## API events
    # 1. Confirm which events and columns I want to keep - pretty close on this
    # 2. Ensure dictionary keys are returned in the correct order
    # 3. Add home and away columns

## HTML events
    # 1. Add the time columns where necessary
    # 2. Ensure dictionary keys are returned in the correct order
    # 3. Change time column to 0:00 format
    # 4. Fix away skaters and home skaters columns

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

from unidecode import unidecode
import re

import math

# These are dictionaries of names that are used throughout the module
from chickenstats.chicken_nhl.info import correct_names_dict, correct_api_names_dict, team_codes
from chickenstats.chicken_nhl.fixes import api_events_fixes, api_rosters_fixes, html_shifts_fixes

############################################## Requests functions & classes ##############################################

# This function & the timeout class are used for scraping throughout

## []Docstring
## []Comments
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

## []Docstring
## []Comments
def s_session():
    '''Creates a requests Session object using the HTTPAdapter from above'''

    s = requests.Session()
    
    retry = urllib3.Retry(total = 5, backoff_factor = 1, respect_retry_after_header = False,
                          status_forcelist=[54, 60, 401, 403, 404, 408, 429, 500, 502, 503, 504])
    
    adapter = TimeoutHTTPAdapter(max_retries = retry, timeout = 3)
    
    s.mount('http://', adapter)
    
    s.mount('https://', adapter)
    
    return s

############################################## General helper functions ##############################################

# These are used in other functions throughout the module

## []Docstring
## []Comments
def scrape_live_endpoint(game_id, session):
    '''Scrapes the live NHL API endpoint. Used to prevent multiple hits to same endpoint during PBP scrape'''

    s = session
    
    url = f'https://statsapi.web.nhl.com/api/v1/game/{game_id}/feed/live'
    
    response = s.get(url).json()
    
    return response

## []Docstring
## []Comments
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

## []Docstring
## []Comments
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

## []Docstring
## []Comments
def convert_ids(api_game_id):
    '''Takes an NHL API ID and converts it to an HTML season and game ID'''

    html_season_id = str(int(str(api_game_id)[:4])) + str(int(str(api_game_id)[:4]) + 1)
    
    html_game_id = str(api_game_id)[5:]
    
    return html_season_id, html_game_id

## []Docstring
## []Comments
def game_id_info(game_id, html_id = False):
    
    if str(game_id).isdigit() == False or len(str(game_id)) != 10:
        
        raise Exception('NOT A VALID GAME ID')
    
    year = int(str(game_id)[0:4])
    
    season = int(f'{year}{year + 1}')
    
    game_session = str(game_id)[4:6]
    
    game_sessions = {'O1': 'PR', '02': 'R', '03': 'P'}
    
    for session_code, session_name in game_sessions.items():
        
        if game_session == session_code:
            
            game_session = session_name
            
    if html_id == True:
        
        html_id = str(game_id)[4:]
        
        return season, game_session, html_id
    
    else:
            
        return season, game_session

def progressbar(message):

    pbar.set_description(f'{message}'.upper())

    ## Adding current time to the progress bar
    
    now = datetime.now()

    current_time = now.strftime("%H:%M:%S")

    postfix_str = f'{current_time}'
    
    pbar.set_postfix_str(postfix_str)

############################################## Schedule ##############################################

## []Refactored
## [x]Docstring
## []Comments
def scrape_schedule(seasons = 2022, game_types = ['R', 'P'], date = None, final_only = False, live_only = False, teams = None, disable_print = False):
      
    '''
    
    --------- INFO ---------

    Scrapes schedule from the NHL API for a given four-digit year or list-like object of four-digit years. Returns a Pandas DataFrame.
    By default, returns entire 2022 schedule, including playoffs, if applicable. 
    NHL historical data supported, 1917-present.

    Typically takes 2-5 seconds per seasons, but can increase if scraping multiple seasons.

    --------- OPTIONAL PARAMETER(S) ---------

    seasons | integer or list-like object: default = 2022
        Four-digit year (e.g., 2022), or list-like object consisting of four-digit years (e.g., generator or Pandas Series)

    game_types | list: default = ['R', 'P']
        Determines the types of games that are returned. Not all game types are supported or have adequate data
        The following are available:
        'R': regular season
        'P': playoffs
        'PR': preseason (not supported)
        'A': all star game (not supported)
        'all': all game types

    date | object: default = None
        Date in 'YYYY-MM-DD' format or 'today'
        If not None, scrapes games from the date given, or today, in your system's local time

    final_only | boolean: default = False
        If True, scrapes only the games that have finished

    live_only | boolean: default = False
        If True, scrapes only live games

    teams | list of team names: default = None
        If not none, filters the schedule to include only the given teams

    disable_print | boolean: default = False
        If True, progress bar is disabled
        If False, prints progress bar

    --------- RETURNS ---------
    
    Pandas DataFrame with columns:
        
        season: string
            Season as 8-digit number, e.g., 20222023 for 2022-23 season

        game_id: integer
            Unique game ID assigned by the NHL

        game_date_dt: datetime
            Datetime in Eastern time zone for game start

        game_date: string
            Date game is / was played (Eastern time)

        start_time: string
            Time game is / was started (Eastern time)
    
        game_type: string
            Whether game is regular season, playoffs, or other, e.g., all-star

        game_status: string
            Whether game is final, currently being played, or scheduled

        home_team: string
            Team name in upper case, no accents (sorry MONTREAL)

        home_team_code: string
            Three-letter team code

        home_team_score: integer
            Goals scored by home team - shootout win is a goal

        away_team: string:
            Team name in upper case, no accents (sorry MONTREAL)

        away_team_code: string
            Three-letter team code

        away_team_score: string
            Goals scored by away team - shootout win is a goal

        detailed_game_status: string
            Whether game is final, currently being played, or scheduled

        start_time_tbd: bool
            If start time is to be determined, then True

        home_team_wins: integer
            Number of wins by home team entering game if not yet played, and exiting game if already played

        home_team_losses: integer
            Number of losses by home team entering game if not yet played, and exiting game if already played

        home_team_otl: integer
            Number of overtime losses by home team entering game if not yet played, and exiting game if already played

        away_team_wins: integer
            Number of wins by away team entering game if not yet played, and exiting game if already played

        away_team_losses: integer
            Number of losses by away team entering game if not yet played, and exiting game if already played

        away_team_otl: integer
            Number of overtime losses by away team entering game if not yet played, and exiting game if already played

        home_team_id: integer
            Unique team / franchise identifier given by NHL

        home_team_link: string
            Link to team information via the NHL API

        away_team_id: integer
            Unique team / franchise identifier given by NHL

        away_team_link: integer
            Link to team information via the NHL API

        venue_name: string
            Name of the venue where game is played 

        venue_id: string
            Unique venue identifier given by the NHL. Will be np.nan if not a venue with regular games

        venue_link: string
            Link to venue information via the NHL API. Will be null if not a venue with regular games

        game_link: string
            Link to detailed game information via the NHL API 

        game_content_link: string
            Link to additional game content via the NHL API

        status_code: string
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

## []Refactored
## [x]Docstring
## []Comments
def scrape_standings(seasons = 2022, disable_print = False):
    
    '''
    
    --------- INFO ---------

    Scrapes standings from the NHL API for a given four-digit year or list-like object of four-digit years. Returns a Pandas DataFrame.
    By default, the standings are for 2022 and are of the moment the function is run, e.g., anything in the past will be historical. 
    NHL historical data supported, 1917-present.

    Scrapes approximately 1 season per second.

    --------- OPTIONAL PARAMETER(S) ---------

    seasons | integer or list-like object: default = 2022
        Four-digit year (e.g., 2022), or list-like object consisting of four-digit years (e.g., generator or Pandas Series)

    disable_print | boolean: default = False
        If True, disables progress bar
        If False, prints progress bar

    --------- RETURNS ---------

    Pandas DataFrame with columns:
        
        season: integer
            Season as 8-digit number, e.g., 20222023 for 2022-23 season

        team: string
            Team name in upper case, no accents (sorry MONTREAL)

        team_code: string
            Three-letter team code
    
        games_played: integer
            Total number of games played

        points: integer
            Standings points earned

        points_percentage: float
            Standings points earned as a percentage of total standings points possible

        win: integer
            Number of wins earned by team

        loss: integer
            Number of losses incurred by team

        otl: integer
            Number of overtime losses incurred by team

        streak: string
            Streak of games team has lost, won, or tied

        league_rank: integer
            League rank by points percentage

        conference_rank: integer
            Conference rank by points percentage

        division_rank: integer
            Division rank by points percentage

        wildcard_rank: integer
            Ranking for wild card playoff spot

        goals_scored: integer
            Number of goals scored

        goals_against: integer
            Number of goals allowed

        team_id: integer
            Unique team ID assigned by the NHL

        team_link: integer
            Link to team information via the NHL API

        division_rank_home: integer
            Division ranking by home points percentage

        division_rank_road: integer
            Division ranking by road points percentage

        division_rank_last10: integer
            Division ranking by points percentage in the last ten games

        conference_rank_home: integer
            Conference ranking by home points percentage

        conference_rank_road: integer
            Conference ranking by road points percentage

        conference_rank_last10: integer
            Conference ranking by points percentage in the last ten games

        league_rank_home: integer
            League ranking by home points percentage

        league_rank_road: integer
            League ranking by road points percentage

        league_rank_last10: integer
            League ranking by points percentage in the last ten games

        pp_rank_division: integer
            Powerplay ranking within division

        pp_rank_conference: integer
            Powerplay ranking within conference

        pp_rank_league: integer
            Powerplay ranking within league

        last_updated: string
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
## [x]Docstring
## [x]Comments
def scrape_game_info(game_ids, live_response = None, session = None, nested = True):
    
    '''

    --------- INFO ---------

    Scrapes game information from the API for a given game ID or list-like object of game IDs.
    Primarily used in combination with other scraping functions, but can be used standalone with nested parameter.

    By default returns a dictionary with game IDs as keys and a dictionary of game information as the values.
    If nested is False, returns a Pandas DataFrame.

    Scrapes approximately 8-12 games per second.

    --------- REQUIRED PARAMETER(S) ---------

    game_ids | integer or list-like object
        A single 1--digit API game ID (e.g., 2022020002) or list-like object of game IDs (e.g., generator or Pandas Series)

    --------- OPTIONAL PARAMTER(S) ---------

    live_response | JSON object: default = None
        When using in another scrape function, can pass the live endpoint response as a JSON object to prevent redundant hits

    session | requests Session object: default = None
        When using in another scrape function, can pass the requests session to improve speed

    nested | boolean: default = True
        If True, progress bar is disabled and a dictionary with game IDs as keys and lists of players as dictionaries
        If False, prints progress to the console and returns a Pandas DataFrame

    --------- RETURNS ---------

    Default: Dictionary with game IDs as keys and a dictionaries of game information as the values

    If nested is False, then returns a Pandas DataFrame, converting the dictionary keys to columns

    The each game info dictionary returned contains the following fields and values:

        season: int
            8-digit season code, e.g., 20222023

        session: object
            Regular season or playoffs, e.g., R

        game_id: int
            10-digit game identifier, e.g., 2022020001

        game_date: object
            Game date, e.g., 2022-10-07, assuming Eastern timezone start time

        start_time: object
            Start time, e.g., 14:00:00, assuming Eastern timezone start time

        home_team: object
            3-letter team code for home team, e.g., NSH

        home_team_name: object
            Full team name for home team, e.g., NASHVILLE PREDATORS

        away_team: object
            3-letter team code for away team, e.g., SJS

        away_team_name: object
            Full team name for away team, e.g., SAN JOSE SHARKS

        game_venue: object
            Name of the venue where game is / was played, e.g., O2 CZECH REPUBLIC

        end_time: object
            Start time, e.g., 16:49:33, assuming Eastern timezone end time

        start_time_dt: datetime object
            Timezone-aware (US/Eastern) datetime object for game start time

        end_time_dt: datetime object
            Timezone-aware (US/Eastern) datetime object for game end time

        home_team_id: integer
            Unique franchise identifier for home team, e.g., 34

        home_team_link: object
            API endpoint for the home team, e.g., /api/v1/franchises/34

        home_team_division: object
            Division name for the home team, e.g., CENTRAL

        home_team_division_id: integer
            Unique ID for home team's division, e.g., 16

        home_team_conference: object
            Name of the home team's conference, e.g., WESTERN

        home_team_conference_id: integer
            Unique ID for home team's conference, e.g., 5

        away_team_id: integer
            Unique franchise identifier for away team, e.g., 29

        away_team_link: object
            API endpoint for the home team, e.g., /api/v1/franchises/29

        away_team_division: object
            Division name for the home team, e.g., PACIFIC

        away_team_division_id: integer
            Unique ID for home team's division, e.g., 15

        away_team_conference: object
            Name of the home team's conference, e.g., WESTERN

        away_team_conference_id: integer
            Unique ID for home team's conference, e.g., 5

        player_name: object
            Player's latin-encoded name, e.g., FILIP FORSBERG

        start_time_utc: datetime object
            Timezone-aware (UTC) datetime object for game start time

        end_time_utc: datetime object
            Timezone-aware (UTC) datetime object for game end time

    '''
    
    ## Convert game IDs to list if given a single game ID

    game_ids = convert_to_list(obj = game_ids, object_type = 'game ID')
    
    ## List to collect the bad games that don't work

    bad_game_list = []

    ## Creating session object to speed up the scraper
        
    if session == None:

        s = s_session()

    else:

        s = session

    ## Creating a dictionary to collect all of the information to eventually be returned
    
    games_dict = {}

    ## Creating progress bar and disabling if function is nested
    
    pbar = tqdm(game_ids, disable = nested)

    ## Iterating through game IDs
    
    for game_id in pbar:

        ## Dictionary to collect all of the game information to be added to the dictionary to be returned 

        game_data = {}

        ## Scraping live endpoint if not already scraped

        if live_response == None:
            
            response = s.get(f'https://statsapi.web.nhl.com/api/v1/game/{game_id}/feed/live').json()

        else:

            response = live_response

        ## If there is no information in the API, continue and add game to the bad list
        
        if response['gameData'] == []:
                
            bad_game_list.append(game_id)
            
            response = None
            
            continue

        ## Game information dictionary

        game_info = response['gameData']['game']

        ## Datetime information dictionary

        dt_info = response['gameData']['datetime']

        ## Game status information dictionary

        status_info = response['gameData']['status']

        ## Team information dictionary

        team_info = response['gameData']['teams']

        ## Venue information dictionary

        venue_info = response['gameData']['venue']

        ## New values to be created for the game information dictionary 

        new_values = {'season': int(game_info['season']),
                      'session': game_info['type'],
                      'game_id': int(game_info['pk']),
                      'game_status': status_info['detailedState'].upper(),
                      'start_time_dt': pd.to_datetime(dt_info.get('dateTime', np.nan)).tz_convert('US/Eastern'),
                      'end_time_dt': pd.to_datetime(dt_info.get('endDateTime', np.nan)).tz_convert('US/Eastern'),
                      'game_venue': unidecode(venue_info['name']).upper(),
                      'home_team': team_info['home']['triCode'].upper().replace('PHX', 'ARI'),
                      'home_team_name': unidecode(team_info['home']['name']).upper().replace('PHOENIX COYOTES', 'ARIZONA COYOTES'),
                      'home_team_id': int(team_info['home']['franchiseId']),
                      'home_team_link': team_info['home']['franchise']['link'],
                      'home_team_division': team_info['home']['division']['name'].upper(),
                      'home_team_division_id': int(team_info['home']['division']['id']),
                      'home_team_conference': team_info['home']['conference']['name'].upper(),
                      'home_team_conference_id': int(team_info['home']['conference']['id']),
                      'away_team': team_info['away']['triCode'].upper().replace('PHX', 'ARI'),
                      'away_team_name': unidecode(team_info['away']['name']).upper().replace('PHOENIX COYOTES', 'ARIZONA COYOTES'),
                      'away_team_id': int(team_info['away']['franchiseId']),
                      'away_team_link': team_info['away']['franchise']['link'],
                      'away_team_division': team_info['away']['division']['name'].upper(),
                      'away_team_division_id': int(team_info['away']['division']['id']),
                      'away_team_conference': team_info['away']['conference']['name'].upper(),
                      'away_team_conference_id': int(team_info['away']['conference']['id']),
                      'start_time_utc': pd.to_datetime(dt_info['dateTime']),
                      'end_time_utc': pd.to_datetime(dt_info.get('endDateTime', np.nan)),
                     }

         ## Adding the new values to the game data dictionary 

        game_data.update(new_values)

        ## Creating dates in string format based on datetime fields

        game_data['game_date'] = game_data['start_time_dt'].strftime('%Y-%m-%d')

        game_data['start_time'] = game_data['start_time_dt'].strftime('%H:%M:%S')

        ## Creating end time in string format based on datetime field, if exists

        if 'endDateTime' not in dt_info.keys():

            game_data['end_time'] = ''

        else:

            game_data['end_time'] = game_data['end_time_dt'].strftime('%H:%M:%S')

        ## If the game data dictionary is empty, continue

        if game_data == {}:

            continue

        ## Update the games dictionary, which is eventually returned, with the game ID and the list of game information 

        games_dict.update({game_id: game_data})

        ## If this is the last game ID, update progress bar information
        
        if game_id == game_ids[-1]:
            
            pbar.set_description(f'Finished scraping game info data'.upper())

            ## If this function is not nested, close the session object

            if nested == False:

                s.close()
            
        else:
        
            pbar.set_description(f'Finished scraping {game_id}'.upper())

        ## Adding time information to the progress bar information 
        
        now = datetime.now()

        current_time = now.strftime("%H:%M:%S")

        postfix_str = f'{current_time}'
        
        pbar.set_postfix_str(postfix_str)

    ## If games dictionary is empty, return empty dictionary or None, based on nested parameter
    
    if games_dict == {}:

        if nested == True:

            return {}

        else:

            return None

    ## If function is not nested, convert dictionary to a dataframe

    if nested == False:

        df = pd.DataFrame(list(games_dict.values()))

        columns = ['season', 'session', 'game_id', 'game_date', 'start_time',
                    'home_team', 'home_team_name', 'away_team', 'away_team_name',
                    'game_venue', 'end_time', 'start_time_dt', 'end_time_dt', 'home_team_id',
                    'home_team_link', 'home_team_division', 'home_team_division_id',
                    'home_team_conference', 'home_team_conference_id', 'away_team_id',
                    'away_team_link', 'away_team_divison', 'away_team_division_id',
                    'away_team_conference', 'away_team_conference_id', 'start_time_utc',
                    'end_time_utc',
                    ]

        columns = [x for x in columns if x in df.columns]

        df = df[columns]

        return df

    ## Else, return the game dictionary 
    
    else:
        
        return games_dict

############################################## API rosters ##############################################

def base_scrape_api_rosters(game_id, live_response = None, session = None):
    '''Function to scrape API rosters data and return a list of player-dictionaries'''

    if session == None:

        s = s_session()

    ## Else reusing session object to speed up scraper

    else:

        s = session

    ## Scraping live endpoint if have not already done so 

    if live_response == None:
        
        response = s.get(f'https://statsapi.web.nhl.com/api/v1/game/{game_id}/feed/live').json()

    else:

        response = live_response

    if response['gameData'] == []:

        roster_info = {}

    else:

        roster_info = response['gameData']['players']

    ## Creating list of player dictionaries 

    players = list(roster_info.values())

    return players

def munge_api_rosters(game_id, players, game_info):
    '''Function to munge a list of player dictionaries, returns a list of player dictionaries'''

    game_rosters = []

    season, game_session = game_id_info(game_id)

    for player in players:

        ## If no name in player, continue

        if ' ' not in player['fullName']:

            continue

        ## Creating new dictionary to hold player information
        
        player_data = {}

        ## Values to update the player dictionary
        
        new_values = {'season': int(season),
                      'session': game_session,
                      'game_id': int(game_id),
                      'game_date': game_info['game_date'],
                      'player_name': unidecode(player['fullName']).upper(),
                      'api_id': int(player['id']),
                      'position': player.get('primaryPosition', {}).get('code', ''),
                      'position_type': player.get('primaryPosition', {}).get('type', '').upper(),
                      'birth_date': player.get('birthDate', ''),
                      'birth_city': unidecode(player.get('birthCity', '')).upper(),
                      'birth_state_province': unidecode(player.get('birthStateProvince', '')).upper(),
                      'birth_country': unidecode(player.get('birthCountry', '')).upper(),
                      'nationality': player.get('nationality', ''),
                      'height': player.get('height', ''),
                      'weight': player.get('weight', ''),
                      'active': player.get('active', 0),
                      'alternate_captain': player.get('alternateCaptain', 0),
                      'captain': player.get('captain', 0),
                      'rookie': player.get('rookie', 0),
                      'roster_status': player.get('rosterStatus', 0),
                      'first_name': unidecode(player.get('firstName', '')).upper(),
                      'last_name': unidecode(player.get('lastName', '')).upper(),
                     }
        
        player_data.update(new_values)

        ## Replacing certain names

        player_data['player_name'] = correct_names_dict.get(player_data['player_name'], player_data['player_name'])

        ## Changing to Alex and Chris

        name_keys = ['player_name', 'first_name']

        for name_key in name_keys:
        
            player_data[name_key] = player_data[name_key].replace('ALEXANDRE', 'ALEX').replace('ALEXANDER', 'ALEX').replace('CHRISTOPHER', 'CHRIS')

        ## Creating ID that matches Evolving Hockey data

        name_split = player_data['player_name'].split(' ', maxsplit = 1)

        player_data['eh_id'] = f"{name_split[0]}.{name_split[1]}"

        player_data['eh_id'] = correct_api_names_dict.get(player_data['api_id'], player_data['eh_id'])

        player_data['eh_id'] = player_data['eh_id'].replace('..', '.')

        ## Adding player handedness information, if exists
        
        if player_data['position'] == 'G':
            
            player_data['catches'] = player.get('shootsCatches', np.nan)
            
        else:
            
            player_data['shoots'] = player.get('shootsCatches', np.nan)

        ## Changing certain columns to binary values
            
        cols = ['active', 'alternate_captain', 'captain', 'rookie', 'roster_status']
        
        for col in cols:
            
            if player_data[col] == True or player_data[col] == 'Y':
                
                player_data[col] = 1
                
            elif player_data[col] == False or player_data[col] != 'Y':
                
                player_data[col] = 0

        ## Calculating player height in feet as a decimal

        if player_data['height'] != '':
                
            height_split = player_data['height'].split("' ")
            
            height_ft = int(height_split[0])
            
            height_in = int(height_split[1].replace('''"''', ''))

            player_data['height'] = height_ft + (height_in / 12)

        ## Shorting position types
        
        position_types = {'FORWARD': 'F', 'DEFENSEMAN': 'D', 'GOALIE': 'G'}
        
        player_data['position_type'] = position_types.get(player_data['position_type'])

        ## Adding age in years

        if player_data['birth_date'] != '':

            player_data['age'] = (pd.to_datetime(game_info['game_date']) - pd.to_datetime(player_data['birth_date'])).days / 365.2425

        ## Appending the player data to the game roster list
        
        game_rosters.append(player_data)

    ## If no game rosters, continue

    return game_rosters

def finalize_api_rosters(games_dict):
    '''Function that preps API rosters data into a dataframe'''

    roster_data = [player for players in list(games_dict.values()) for player in players]

    df = pd.DataFrame(roster_data)

    columns = ['season', 'session', 'game_id', 'game_date', 'player_name', 'api_id', 'eh_id',
                'position', 'position_type', 'birth_date', 'age', 'birth_city',
                'birth_state_province', 'birth_country', 'nationality', 'height',
                'weight', 'shoots', 'catches', 'first_name', 'last_name', 
                'roster_status', 'active', 'rookie', 'alternate_captain',
                'captain', ]

    columns = [x for x in columns if x in df.columns]

    df = df[columns]

    df = df.replace('', np.nan).replace(' ', np.nan)

    return df

def scrape_api_rosters(game_ids, live_response = None, session = None, nested = True):
    
    '''

    --------- INFO ---------

    Scrapes the game roster from the API for a given game ID or list-like object of game IDs.
    Primarily used in combination with other scraping functions, but can be used standalone with nested parameter.

    By default returns a dictionary with game IDs as keys and lists of player-dictionaries as values.
    If nested is False, returns a Pandas DataFrame.

    Scrapes approximately 8-12 games per second.

    --------- REQUIRED PARAMETER(S) ---------

    game_ids | integer or list-like object
        A single 10-digit API game ID (e.g., 2022020002) or list-like object of 10-digit game IDs (e.g., generator or Pandas Series)

    --------- OPTIONAL PARAMETER(S) ---------

    live_response | JSON object: default = None
        When using in another scrape function, can pass the live endpoint response as a JSON object to prevent redundant hits

    session | requests Session object: default = None
        When using in another scrape function, can pass the requests session to improve speed

    nested | boolean: default = True
        If True, progress bar is disabled and a dictionary with game IDs as keys and lists of players as dictionaries
        If False, prints progress to the console and returns a Pandas DataFrame


    --------- RETURNS ---------

    Default: Dictionary with game IDs as keys and lists of player-dictionaries as the values.

    If nested = False, then returns a Pandas DataFrame, converting the dictionary keys to columns.

    The each game info dictionary returned contains the following fields and values:

        season: int
            8-digit season code, e.g., 20222023

        session: object
            Regular season or playoffs, e.g., R

        game_id: int
            10-digit game identifier, e.g., 2022020001

        player_name: object
            Player's full name, e.g., FILIP FORSBERG

        api_id: integer
            Unique 7-digit player identifier, e.g., 8476887

        eh_id: object
            Unique identifier that matches Evolving Hockey, e.g., FILIP.FORSBERG

        position: object
            Player's position, e.g., L

        position_type: object
            Player's position group, e.g., F

        birth_date: object
            Player's birth date, e.g., 1994-08-13

        birth_city: object
            Player's birth city, e.g., OSTERVALA

        birth_state_province: object
            U.S. state or Canadian province where player was born, if applicable, e.g., NaN

        birth_country: object
            Country where player was born, e.g., SWE

        nationality: float
            Player's nationality, e.g., SWE

        height: float
            Player's height in feet, e.g., 6.083333

        weight: int
            Players weight in pounds, e.g., 205

        shoots: object
            Skater's shooting hand, e.g., R

        catches: object
            Goalie's catching hand, e.g., NaN

        first_name: object
            Player's first name, e.g., FILIP

        last_name: object
            Player's last name, e.g., FORSBERG

        roster_status: integer
            Whether player is active for game, e.g., 1

        active: integer
            Whether player is currently active, e.g., 1

        rookie: integer
            Whether player is a rookie, e.g., 0

        alternate_captain: integer
            Whether player is designated an alternate captain, e.g., 0

        captain: integer
            Whether player is designated the captain, e.g., 0

    '''
    
    ## Convert game IDs to list if given a single game ID

    game_ids = convert_to_list(obj = game_ids, object_type = 'game ID')
    
    ## List to collect games that don't have API information 

    bad_game_list = []

    ## Creating session object if doesn't exist
        
    if session == None:

        s = s_session()

    ## Else reusing session object to speed up scraper

    else:

        s = session

    ## Creating dictionary to collect information that will eventually be returned
    
    games_dict = {}

    ## Creating progress bar
    
    pbar = tqdm(game_ids, disable = nested)

    ## Iterating through game IDs
    
    for game_id in pbar:

        ## Scraping live endpoint if have not already done so 

        if live_response == None:
            
            response = s.get(f'https://statsapi.web.nhl.com/api/v1/game/{game_id}/feed/live').json()

        else:

            response = live_response

        ## If no information in the live response, continue

        game_rosters = base_scrape_api_rosters(game_id, live_response = response, session = s)

        if game_rosters == []:

            pbar_message = f'NO API ROSTER DATA FOR {game_id}'.upper()

            ## Adding current time to the progress bar

            progressbar(pbar_message)

            continue

        game_info = scrape_game_info(game_id, live_response = response, session = s, nested = True)[game_id]

        game_rosters = munge_api_rosters(game_id, game_rosters, game_info)

        game_rosters = api_rosters_fixes(game_id, game_rosters)

        games_dict.update({game_id: game_rosters})

        ## If this is the last game ID, changing progress bar information
        
        if game_id == game_ids[-1]:
            
            pbar_message = f'FINISHED SCRAPING API ROSTER DATA'.upper()

            ## If function is not nested, closing the session object

            if nested == False:

                s.close()
            
        else:
        
            pbar_message = f'FINISHED SCRAPING {game_id}'.upper()

        ## Adding current time to the progress bar

        progressbar(pbar_message)

    ## Continuing if rosters dict is empty and returning values based on nested parameter
    
    if games_dict == {}:

        if nested == True:

            return {}

        else:

            return None

    ## If not nested, return a dataframe

    if nested == False:

        df = finalize_api_rosters(games_dict)

        return df

    ## Else return a dictionary with game IDs as keys and lists of player-dictionaries as values
    
    else:
        
        return games_dict

############################################## HTML rosters ##############################################

## [x]Refactored
## [x]Docstring
## [x]Comments
def scrape_html_rosters(game_ids, session = None, nested = True):
    
    '''

    --------- INFO ---------

    Scrapes the rosters from the HTML endpoint for a given game ID or list-like object of game IDs.
    Primarily used in combination with other scraping functions, but can be used standalone with nested parameter.

    By default returns a dictionary with game IDs as keys and lists of player-dictionaries as values.
    If nested is False, returns a Pandas DataFrame.

    Scrapes approximately 4-8 games per second.

    --------- REQUIRED PARAMETERS ---------

    game_ids | integer or list-like object
        A single 10-digit API game ID (e.g., 2022020002) or list-like object of 10-digit game IDs (e.g., generator or Pandas Series)

    --------- OPTIONAL PARAMETERS ---------

    session | requests Session object: default = None
        When using in another scrape function, can pass the requests session to improve speed

    nested | boolean: default = True
        If True, progress bar is disabled and a dictionary with game IDs as keys and lists of players as dictionaries
        If False, prints progress to the console and returns a Pandas DataFrame

    --------- RETURNS ---------

    Default: Dictionary with game IDs as keys and lists of player-dictionaries as the values

    If nested = False, then returns a Pandas DataFrame, converting the dictionary keys to columns.

    Each player in each game list is a dictionary with the following fields and values:

        season: integer
            8-digit season code, e.g., 20222023

        session: object
            Regular season or playoffs, e.g., R

        game_id: integer
            10-digit game identifier, e.g., 2022020001

        team: object
            3-letter team code, e.g., NSH

        team_name: object
            Full team name, e.g., NASHVILLE PREDATORS

        team_venue: object
            Whether team is home or away, e.g., home

        player_name: object
            Player's latin-encoded name, e.g., FILIP FORSBERG

        eh_id: object
            Identifier that can be used to match with Evolving Hockey data, e.g., FILIP.FORSBERG

        team_jersey: object
            3-letter team code plus player's jersey number, e.g., NSH9
            Used for identification in other functions

        jersey: integer
            Player's jersey number, e.g., 9

        position: object
            Player's position, e.g., F

        starter: integer
            Whether player started the game, e.g., 1

        status: object
            Player's status, e.g., ACTIVE

    '''
    
    ## Converting game IDs to a list if it is a single game ID

    game_ids = convert_to_list(obj = game_ids, object_type = 'game ID')
    
    ## Creating session object if doesn't already exist

    if session == None:
    
        s = s_session()

    ## Else reusing session object to speed up scraper
        
    else:
        
        s = session

    ## Creating dictionary to collect values that will eventually be returned
    
    games_dict = {}

    ## Creating progress bar
    
    pbar = tqdm(game_ids, disable = nested)

    ## Iterating through game IDs
    
    for game_id in pbar:

        ## Creating season and html_game_id from game_id
        
        season, html_game_id = convert_ids(game_id)

        ## Creating game session information from game_id

        game_session = str(game_id)[4:6]

        if game_session == '01':

            game_session = 'PR'

        if game_session == '02':

            game_session = 'R'

        if game_session == '03':

            game_session == 'P'

        ## URL and scraping url
    
        url = f'http://www.nhl.com/scores/htmlreports/{season}/RO0{html_game_id}.HTM'
        
        page = s.get(url)

        ## Continue if status code is bad
        
        page_status = page.status_code
        
        if page_status == 404:

            pbar.set_description(f'{game_id} not available')

            now = datetime.now()

            current_time = now.strftime("%H:%M:%S")

            postfix_str = f'{current_time}'

            pbar.set_postfix_str(postfix_str)
            
            continue
        
        ## Dictionaries for reading the HTML data

        td_dict = {'align':'center', 'class':['teamHeading + border', 'teamHeading + border '], 'width':'50%'}

        table_dict = {'align':'center', 'border':'0', 'cellpadding':'0', 'cellspacing':'0', 'width':'100%', 'xmlns:ext':''}

        ## Reading the HTML file sing beautiful soup package

        soup = BeautifulSoup(page.content.decode('ISO-8859-1'), 'lxml', multi_valued_attributes = None)

        ## Information for reading the HTML data

        td_dict = {'align':'center', 'class':['teamHeading + border', 'teamHeading + border '], 'width':'50%'}

        ## Finding all active players in the html file

        teamsoup = soup.find_all('td', td_dict)

        ## Dictionary for finding each team's table in the HTML file

        table_dict = {'align':'center', 'border':'0', 'cellpadding':'0', 'cellspacing':'0', 'width':'100%', 'xmlns:ext':''}

        ## Dictionary to collect the team names

        team_names = {}

        ## Dictionary to collect the team tables from the HTML data for iterating

        team_soup_list = []

        ## List of teams for iterating

        team_list = ['away', 'home']

        ## List to collect the player dictionaries during iteration

        player_list = []

        ## Iterating through the home and away teams to collect names and tables

        for idx, team in enumerate(team_list):
            
            ## Collecting team names
            
            team_name = unidecode(teamsoup[idx].get_text().encode('latin-1').decode('utf-8')).upper()
            
            ## Correcting the Coyotes team name
            
            if team_name == 'PHOENIX COYOTES':
                
                team_name = 'ARIZONA COYOTES'

            team_names.update({team : team_name})
            
            ## Collecting tables of active players

            team_soup_list.append((soup.find_all('table', table_dict))[idx].find_all('td'))
            
        ## Itereating through the team's tables of active players

        for idx, team_soup in enumerate(team_soup_list):

            table_dict = {'align':'center', 'border':'0', 'cellpadding':'0', 'cellspacing':'0', 'width':'100%', 'xmlns:ext':''}

            stuff = soup.find_all('table', table_dict)[idx].find_all("td", {"class" : "bold"})

            starters = list(np.reshape(stuff, (int(len(stuff) / 3), 3))[:, 2])
            
            ## Getting length to create numpy array

            length = int(len(team_soup) / 3)
            
            ## Creating a numpy array from the data, chopping off the headers to create my own
            
            active_array = np.array(team_soup).reshape(length, 3)
            
            ## Getting original headers
            
            og_headers = active_array[0]
            
            if 'Name' not in og_headers and 'Nom/Name' not in og_headers:
                
                continue
            
            ## Chop off the headers to create my own
            
            actives = active_array[1:]
            
            ## Iterating through each player, or row in the array
            
            for player in actives:
                
                ## New headers for the data. Original headers | ['#', 'Pos', 'Name']
                
                if len(player) == 3:

                    headers = ['jersey', 'position', 'player_name']
                    
                ## Sometimes headers are missing
                
                elif len(player) == 2:
                    
                    headers = ['jersey', 'player_name']
                
                ## Creating dictionary with headers as keys from the player data
                
                player = dict(zip(headers, player))
                
                ## Adding new values to the player dictionary
                
                new_values = {'team_name': team_names.get(team_list[idx]),
                              'team_venue': team_list[idx].upper(),
                              'status': 'ACTIVE'}

                if player['player_name'] in starters:

                    player['starter'] = 1

                else:

                    player['starter'] = 0
                
                if 'position' not in headers:

                    player['position'] = np.nan
                
                ## Update the player's dictionary with new values
                
                player.update(new_values)
                
                ## Append player dictionary to list of players
                
                player_list.append(player)
                
        ## Check if scratches are present

        if len(soup.find_all('table', table_dict)) > 2:
            
            ## If scratches are present, iterate through the team's scratch tables

            for idx, team in enumerate(team_list):
                
                ## Getting team's scratches from HTML

                scratch_soup = (soup.find_all('table', table_dict))[idx + 2].find_all('td')
                
                ## Checking to see if there is at least one set of scratches (first row are headers)

                if len(scratch_soup) > 1:
                    
                    ## Getting the number of scratches

                    length = int(len(scratch_soup) / 3)
                    
                    ## Creating numpy array of scratches, removing headers

                    scratches = np.array(scratch_soup).reshape(length, 3)[1:]
                    
                    ## Iterating through the array
            
                    for player in scratches:
                    
                        ## New headers for the data. Original headers | ['#', 'Pos', 'Name']
                
                        if len(player) == 3:

                            headers = ['jersey', 'position', 'player_name']
                            
                        ## Sometimes headers are missing
                        
                        elif len(player) == 2:
                            
                            headers = ['jersey', 'player_name']
                        
                        ## Creating dictionary with headers as keys from the player data
                        
                        player = dict(zip(headers, player))
                        
                        ## Adding new values to the player dictionary
                        
                        new_values = {'team_name': team_names.get(team_list[idx]),
                                      'team_venue': team_list[idx].upper(),
                                      'starter': 0,
                                      'status': 'SCRATCH'}
                        
                        if 'position' not in headers:

                            player['position'] = np.nan
                        
                        ## Updating player dictionary

                        player.update(new_values)
                        
                        ## Appending the player dictionary to the player list

                        player_list.append(player)

        ## Iterating through each player to change information
                    
        for player in player_list:

            ## Fixing jersey data type

            player['jersey'] = int(player['jersey'])
            
            ## Adding new values in a batch

            new_values = {'season': int(season),
                          'session': game_session,
                          'game_id': game_id}
            
            player.update(new_values)
            
            ## Correcting player names
            
            player['player_name'] = re.sub('\(\s?(.*)\)', '', player['player_name'])\
                                        .strip()\
                                        .encode('latin-1')\
                                        .decode('utf-8')\
                                        .upper()
            
            ## Replacing certain names

            player['player_name'] = unidecode(player['player_name'])

            player['player_name'] = player['player_name'].replace('ALEXANDRE', 'ALEX').replace('ALEXANDER', 'ALEX').replace('CHRISTOPHER', 'CHRIS')

            player['player_name'] = correct_names_dict.get(player['player_name'], player['player_name'])
            
            ## Creating Evolving Hockey ID
            
            player['eh_id'] = unidecode(player['player_name'])

            ## List of names and fixed from Evolving Hockey Scraper.
            #player['eh_id'] = correct_names_dict.get(player['eh_id'], player['eh_id'])
            
            name_split = player['eh_id'].split(' ', maxsplit = 1)
            
            player['eh_id'] = f'{name_split[0]}.{name_split[1]}'

            player['eh_id'] = player['eh_id'].replace('..', '.')
            
            ## Correcting Evolving Hockey IDs for duplicates
            
            duplicates = {'SEBASTIAN.AHO': player['position'] == 'D',
                          'COLIN.WHITE': player['season'] >= 20162017,
                          'SEAN.COLLINS': player['position'] != 'D',
                          'ALEX.PICARD': player['position'] != 'D',
                          'ERIK.GUSTAFSSON': player['season'] >= 20152016,
                          'MIKKO.LEHTONEN': player['season'] >= 20202021,
                          'NATHAN.SMITH': player['season'] >= 20212022,
                          'DANIIL.TARASOV': player['position'] == 'G'
                         }
            
            ## Iterating through the duplicate names and conditions
            
            for duplicate_name, condition in duplicates.items():
                
                if player['eh_id'] == duplicate_name and condition:
                    
                    player['eh_id'] = f'{duplicate_name}2'
                    
            ## Something weird with Colin White
            
            if player['eh_id'] == 'COLIN.':
                
                player['eh_id'] = 'COLIN.WHITE2'

            player['team'] = team_codes.get(player['team_name'])

            player['team_jersey'] = f"{player['team']}{player['jersey']}"

        ## Adding roster information to dictionary that is eventually returned

        games_dict.update({game_id: player_list})

        ## Changing progress bar information if this is the last game ID
        
        if game_id == game_ids[-1]:
            
            pbar.set_description(f'Finished scraping roster data')

            ## Closing session object if not nested

            if nested == False:

                s.close()

        else:
        
            pbar.set_description(f'Finished scraping {game_id}')

        ## Adding time information to progress bar
        
        now = datetime.now()

        current_time = now.strftime("%H:%M:%S")

        postfix_str = f'{current_time}'
        
        pbar.set_postfix_str(postfix_str)

    ## If not nested, returns a dataframe
    
    if nested == False:

        roster_data = [player for players in games_dict.values() for player in players]

        df = pd.DataFrame(roster_data)

        column_order = ['season', 'session', 'game_id', 'team', 'team_name', 'team_venue',
                        'player_name', 'eh_id', 'team_jersey', 'jersey', 'position', 'starter',
                        'status']

        column_order = [x for x in column_order if x in df.columns]

        df = df[column_order]

        return df

    ## Else returns a dictionary with game IDs as keys and lists of player-dictionaries as values

    else:

        return games_dict

## [x]Refactored
## [x]Docstring
## []Comments
def scrape_rosters(game_ids, html_rosters = None, api_rosters = None, session = None, nested = True):
    '''

    --------- INFO ---------

    Scrapes and cmbines the rosters from the HTML and API endpoints for a given game ID or list-like object of game IDs.
    Primarily used in combination with other scraping functions, but can be used standalone with nested parameter.

    By default returns a dictionary with game IDs as keys and lists of player-dictionaries as values.
    If nested is False, returns a Pandas DataFrame.

    Scrapes approximately 4-7 games per second.   

    --------- REQUIRED PARAMETER(S) ---------

    game_ids | integer or list-like object
        A single 10-digit API game ID (e.g., 2022020002) or list-like object of 10-digit game IDs (e.g., generator or Pandas Series)

    --------- OPTIONAL PARAMETER(S) ---------

    html_rosters | dictionary from scrape_html_rosters function: default = None
        If nested, can provide HTML rosters to combine without scraping

    api_rosters | dictionary from scrape_api_rosters function: default = None
        If nested, can provide API rosters to combine without scraping

    session | requests Session object: default = None
        When using in another scrape function, can pass the requests session to improve speed

    nested | boolean: default = True
        If True, progress bar is disabled and returns a dictionary with game IDs as keys and lists of shifts as dictionaries 
        If False, prints progress to the console and returns a dataframe
        
    --------- RETURNS ---------

    Default: Dictionary with game IDs as keys and lists of player-dictionaries as the values

    If nested = False, then returns a Pandas DataFrame, converting the dictionary keys to columns.

    Each player in each game list is a dictionary with the following fields and values:

        season: integer
            8-digit season code, e.g., 20222023

        session: object
            Regular season or playoffs, e.g., R

        game_id: integer
            10-digit game identifier, e.g., 2022020001

        team: object
            3-letter team code, e.g., NSH

        team_name: object
            Full team name, e.g., NASHVILLE PREDATORS

        team_venue: object
            Whether team is home or away, e.g., home

        player_name: object
            Player's latin-encoded name, e.g., FILIP FORSBERG

        eh_id: object
            Identifier that can be used to match with Evolving Hockey data, e.g., FILIP.FORSBERG

        api_id: integer
            Unique 7-digit player identifier, e.g., 8476887

        team_jersey: object
            3-letter team code plus player's jersey number, e.g., NSH9
            Used for identification in other functions

        jersey: integer
            Player's jersey number, e.g., 9

        position: object
            Player's position, e.g., F

        shoots: object
            Skater's shooting hand, e.g., R

        catches: object
            Goalie's catching hand, e.g., NaN

        height: float
            Player's height in feet, e.g., 6.083333

        weight: int
            Players weight in pounds, e.g., 205

        starter: integer
            Whether player started the game, e.g., 1

        rookie: integer
            Whether player is a rookie, e.g., 0

        captain: integer
            Whether player is designated the captain, e.g., 0

        alternate_captain: integer
            Whether player is designated an alternate captain, e.g., 0

        status: object
            Player's status, e.g., ACTIVE

        active: integer
            Whether player is currently active, e.g., 1

        birth_date: object
            Player's birth date, e.g., 1994-08-13

        birth_city: object
            Player's birth city, e.g., OSTERVALA

        birth_state_province: object
            U.S. state or Canadian province where player was born, if applicable, e.g., NaN

        birth_country: object
            Country where player was born, e.g., SWE

        nationality: float
            Player's nationality, e.g., SWE

    '''

    
    ## Convert game IDs to list if given a single game ID

    game_ids = convert_to_list(obj = game_ids, object_type = 'game ID')

    ## Dictionary to collect values that will eventually be returned

    games_dict = {}

    ## Creating session object if doesn't already exist
    
    if session == None:
    
        s = s_session()

    ## Else reusing session object to speed up scraper
        
    else:
        
        s = session

    ## Creating progress bar
        
    pbar = tqdm(game_ids, disable = nested)

    ## Iterating through game IDs
    
    for game_id in pbar:

        if api_rosters is None:
            
            api_roster = scrape_api_rosters(game_id, session = s, nested = True)[game_id]

        else:

            api_roster = api_rosters[game_id].copy()

        api_roster = {x['eh_id']: x for x in api_roster}

        ## If no roster data, scrape the HTML rosters
            
        if html_rosters is None:
            
            html_roster = scrape_html_rosters(game_id, session = s, nested = True)[game_id]

        ## If already exists, reuse roster data to prevent redundant hits
            
        else:
            
            html_roster = html_rosters[game_id].copy()

        game_list = []
        
        for player in html_roster:
            
            player_data = {}
            
            player_data.update(player)
            
            if player['eh_id'] not in api_roster.keys():
                
                game_list.append(player_data)
                
                continue
            
            player_api = api_roster[player['eh_id']]
            
            new_values = ['api_id',
                          'birth_date',
                          'age',
                          'birth_city',
                          'birth_state_province',
                          'birth_country',
                          'nationality',
                          'height',
                          'weight',
                          'active',
                          'alternate_captain',
                          'captain',
                          'rookie',
                          'shoots',
                          'catches'
                          
                          ]
            
            new_values = {k: player_api[k] for k in new_values if k in player_api.keys()}
        
            player_data.update(new_values)
            
            game_list.append(player_data)
        
        games_dict.update({game_id: game_list})

        if game_id == game_ids[-1]:
            
            pbar.set_description(f'Finished scraping roster data')

            ## Closing session object if not nested

            if nested == False:

                s.close()

        else:
        
            pbar.set_description(f'Finished scraping {game_id}')

        ## Adding time information to progress bar
        
        now = datetime.now()

        current_time = now.strftime("%H:%M:%S")

        postfix_str = f'{current_time}'
        
        pbar.set_postfix_str(postfix_str)

    ## If not nested, returns a dataframe
    
    if nested == False:

        roster_data = [player for players in games_dict.values() for player in players]

        df = pd.DataFrame(roster_data)

        column_order = ['season', 'session', 'game_id', 'team', 'team_name', 'team_venue',
                        'player_name', 'eh_id', 'api_id','team_jersey', 'jersey', 'position',
                        'shoots', 'catches', 'age', 'height', 'weight', 'starter', 'rookie',
                        'captain', 'alternate_captain', 'status', 'active', 'birth_date',
                        'birth_city', 'birth_state_province', 'birth_country', 'nationality',]

        column_order = [x for x in column_order if x in df.columns]

        df = df[column_order]

        return df

    ## Else returns a dictionary with game IDs as keys and lists of player-dictionaries as values

    else:

        return games_dict

############################################## HTML shifts ##############################################

## [x]Refactored
## [x]Docstring
## [x]Comments
def scrape_shifts(game_ids, roster_data = None, session = None, nested = True):
    
    '''

    --------- INFO ---------

    Scrapes the shifts from the HTML endpoint for a given game ID or list-like object of game IDs.
    Primarily used in combination with other scraping functions, but can be used standalone with nested parameter.

    By default returns a dictionary with game IDs as keys and lists of shift-dictionaries as values.
    If nested is False, returns a Pandas DataFrame.

    Scrapes approximately 1-2 games per second.   

    --------- REQUIRED PARAMETER(S) ---------

    game_ids | integer or list-like object
        A single 10-digit API game ID (e.g., 2022020002) or list-like object of 10-digit game IDs (e.g., generator or Pandas Series)

    --------- OPTIONAL PARAMETER(S) ---------

    roster_data | dictionary: default = None
        If nested in other functions, can provide roster data from scrape function to prevent redundant hits to endpoints
        If standalone, will scrape roster data for each game automatically

    session | requests Session object: default = None
        When using in another scrape function, can pass the requests session to improve speed

    nested | boolean: default = True
        If True, progress bar is disabled and returns a dictionary with game IDs as keys and lists of shifts as dictionaries 
        If False, prints progress to the console and returns a dataframe
        
    --------- RETURNS ---------

    Default: Dictionary with game IDs as keys and lists of shift-dictionaries as the values

    If nested = False, then returns a Pandas DataFrame, converting the dictionary keys to columns.

    Each shift in each game list is a dictionary with the following fields and values:

        season: integer
            8-digit season code, e.g., 20222023

        session: object
            Regular season or playoffs, e.g., R

        game_id: integer
            10-digit game identifier, e.g., 2022020001

        team: object
            3-letter team code, e.g., NSH

        team_name: object
            Full team name, e.g., NASHVILLE PREDATORS

        team_venue: object
            Whether team is home or away, e.g., home

        player_name: object
            Player's latin-encoded name, e.g., FILIP FORSBERG

        eh_id: object
            Identifier that can be used to match with Evolving Hockey data, e.g., FILIP.FORSBERG

        team_jersey: object
            3-letter team code plus player's jersey number, e.g., NSH9
            Used for identification in other functions

        position: object
            Player's position, e.g., L

        jersey: integer
            Player's jersey number, e.g., 9

        shift_count: integer
            Cumulative number of shifts, e.g., 1

        period: integer
            Game period, e.g., 1

        start_time: object
            Clock time (ascending) shift was started, e.g., 0:00

        end_time: object
            Clock time (ascending) shift was ended, e.g., 0:24

        duration: object
            Clock time (ascending) of shift duration, e.g., 0:24

        start_time_seconds: integer
            Start time of shift in seconds, e.g., 0

        end_time_seconds: integer
            End time of shift in seconds, e.g., 24

        duration_seconds: integer
            Duration of shifts in seconds, e.g., 24

        shift_start: object
            Ascending and descending clock times for shift start, e.g, 0:00 / 20:00

        shift_end: object
            Ascending and descending clock times for shift end, e.g, 0:24 / 19:36

        goalie: integer
            Whether player is a goalie or not, e.g., 0

        home: integer
            Wehther player is home or not, e.g., 1

    '''

    
    ## Convert game IDs to list if given a single game ID

    game_ids = convert_to_list(obj = game_ids, object_type = 'game ID')

    ## Dictionary to collect values that will eventually be returned

    games_dict = {}

    ## Creating session object if doesn't already exist
    
    if session == None:
    
        s = s_session()

    ## Else reusing session object to speed up scraper
        
    else:
        
        s = session

    ## Creating progress bar
        
    pbar = tqdm(game_ids, disable = nested)

    ## Iterating through game IDs
    
    for game_id in pbar:

        ## If no roster data, scrape the HTML rosters
            
        if roster_data is None:
            
            roster = scrape_html_rosters(game_id, session = s, nested = True)[game_id]

        ## If already exists, reuse roster data to prevent redundant hits
            
        else:
            
            roster = roster_data[game_id].copy()

        ## Get active players and store them in a new dictionary with team jersey as key and other info as a value-dictionary

        actives = {x['team_jersey']: x for x in roster if x['status'] == 'ACTIVE'}

        scratches = {x['team_jersey']: x for x in roster if x['status'] == 'SCRATCH'}

        ## Creating basic information from game ID
            
        year = int(str(game_id)[0:4])

        season = int(f'{year}{year + 1}')

        html_game_id = str(game_id)[5:]

        game_session = str(game_id)[4:6]

        if game_session == '01':
            
            game_session = 'PR'
            
        if game_session == '02':
            
            game_session = 'R'
            
        if game_session == '03':
            
            game_session == 'P'

        ## This is the list for collecting all of the game information for the end

        game_list = []

        ## Dictionary of urls for scraping

        urls_dict = {'HOME': f'http://www.nhl.com/scores/htmlreports/{season}/TH0{html_game_id}.HTM',
                     'AWAY': f'http://www.nhl.com/scores/htmlreports/{season}/TV0{html_game_id}.HTM'}

        ## Iterating through the url dictionary 

        for team_venue, url in urls_dict.items():

            response = requests.get(url)

            soup = BeautifulSoup(response.content.decode('ISO-8859-1'), 'lxml', multi_valued_attributes = None)

            ## Getting team names from the HTML Data

            team_name = soup.find('td', {'align':'center', 'class':'teamHeading + border'})

            ## Converting team names to proper format

            team_name = unidecode(team_name.get_text())
            
            if team_name == 'PHOENIX COYOTES':
                
                team_name = 'ARIZONA COYOTES'
                
            elif 'CANADIENS' in team_name:
                
                team_name = 'MONTREAL CANADIENS'

            ## Getting players from the HTML data

            players = soup.find_all('td', {'class':['playerHeading + border', 'lborder + bborder']})

            ## Creating a dictionary to collect the players' information

            players_dict = {}

            ## Iterating through the players

            for player in players:

                ## Getting player's data

                data = player.get_text()

                ## If there is a name in the data, get the information

                if ', ' in data:

                    name = data.split(',', 1)

                    jersey = name[0].split(' ')[0].strip()

                    last_name = name[0].split(' ', 1)[1].strip()

                    first_name = re.sub('\(\s?(.+)\)', '', name[1]).strip()

                    full_name = f'{first_name} {last_name}'

                    if full_name == ' ': 

                        continue
                        
                    new_values = {full_name: {'player_name': full_name,
                                              'jersey': jersey,
                                              'shifts': [],
                                             }}

                    players_dict.update(new_values)

                ## If there is not a name it is likely because these are shift information, not player information

                else:

                    ## However, if it is a player info row, without a name, then continue

                    if full_name == ' ': 

                        continue

                    ## Extend the player's shift information with the shift data 

                    players_dict[full_name]['shifts'].extend([data])

            ## Iterating through the player's dictionary, which has a key of the player's name and an array of shift-arrays

            for player, shifts in players_dict.items():

                ## Getting the number of shifts

                length = int(len(np.array(shifts['shifts'])) / 5)

                ## Reshaping the shift data into fields and values

                for number, shift in enumerate(np.array(shifts['shifts']).reshape(length, 5)):

                    ## Adding header values to the shift data

                    headers = ['shift_count', 'period', 'shift_start', 'shift_end', 'duration']

                    ## Creating a dictionary from the headers and the shift data

                    shift_dict = dict(zip(headers, shift.flatten()))

                    ## Adding other data to the shift dictionary
                    
                    new_values = {'season': season,
                                  'session': game_session,
                                  'game_id': game_id,
                                  'team_name': team_name,
                                  'team': team_codes[team_name],
                                  'team_venue': team_venue.upper(),
                                  'player_name': shifts['player_name'],
                                  'eh_id': actives.get(f"{team_codes[team_name]}{shifts['jersey']}",
                                                            scratches.get(f"{team_codes[team_name]}{shifts['jersey']}"))['eh_id'],
                                  'team_jersey': f"{team_codes[team_name]}{shifts['jersey']}",
                                  'position': actives.get(f"{team_codes[team_name]}{shifts['jersey']}",
                                                            scratches.get(f"{team_codes[team_name]}{shifts['jersey']}"))['position'],
                                  'jersey': int(shifts['jersey']),
                                  'period': int(shift_dict['period'].replace('OT', '4').replace('SO', '5')),
                                  'shift_count': int(shift_dict['shift_count']),
                                  'shift_start': unidecode(shift_dict['shift_start']).strip(),
                                  'start_time': unidecode(shift_dict['shift_start']).strip().split('/', 1)[0],
                                  'shift_end': unidecode(shift_dict['shift_end']).strip(),
                                  'end_time': unidecode(shift_dict['shift_end']).strip().split('/', 1)[0],
                                 }
                    
                    shift_dict.update(new_values)

                    ## Appending the shift dictionary to the list of shift dictionaries

                    game_list.append(shift_dict)

        ## Iterating through the lists of shifts
                    
        for shift in game_list:

            ## Replacing some player names

            shift['player_name'] = shift['player_name'].replace('ALEXANDRE', 'ALEX').replace('ALEXANDER', 'ALEX').replace('CHRISTOPHER', 'CHRIS')

            shift['player_name'] = shift.get(shift['player_name'], shift['player_name'])

            ## Replacing period information

            #shift['period'] = int(shift['period'].replace('OT', '4').replace('SO', '5'))

            ## Adding player identifying information
            
            shift['team_jersey'] = shift['team'] + str(shift['jersey'])

            ## Adding seconds columns

            cols = ['start_time', 'end_time', 'duration']

            for col in cols:

                time_split = shift[col].split(':', 1)

                ## Sometimes the shift value can be blank, if it is, we'll skip the field and fix later

                try:

                    shift[f'{col}_seconds'] = 60 * int(time_split[0]) + int(time_split[1])

                except ValueError:

                    continue

            ## Fixing end time if it is blank or empty

            if shift['end_time'] == ' ' or shift['end_time'] == '':

                ## Calculating end time based on duration seconds

                shift['end_time_seconds'] = shift['start_time_seconds'] + shift['duration_seconds']

                ## Creating end time based on time delta

                shift['end_time'] = str(timedelta(seconds=shift['end_time_seconds'])).split(':', 1)[1]

            ## If the shift start is after the shift end, we need to fix the error

            if shift['start_time_seconds'] > shift['end_time_seconds']:

                ## Creating new values based on game session and period

                if shift['period'] < 4:

                    ## Setting the end time

                    shift['end_time'] = '20:00'

                    ## Setting the end time in seconds

                    shift['end_time_seconds'] = 1200

                    ## Setting the shift end

                    shift['shift_end'] = '20:00 / 0:00'

                    ## Setting duration and duration in seconds

                    shift['duration_seconds'] = shift['end_time_seconds'] - shift['start_time_seconds'] 

                    shift['duration'] = str(timedelta(seconds = shift['duration_seconds'])).split(':', 1)[1]

                else:

                    if game_session != 'P':
                            
                            total_seconds = 300
                            
                    elif game_session == 'P':
                        
                        total_seconds = 1200

                    ## Need to get the end period to get the end time in seconds

                    max_period = max([int(shift['period']) for shift in game_list if shift['period'] != ' '])

                    ## Getting the end time in seconds for the final period

                    max_seconds = max([shift['end_time_seconds'] for shift in game_list if 'end_time_seconds' in shift.keys() and shift['period'] == max_period])

                    shift['end_time_seconds'] = max_seconds

                    ## Setting end time

                    end_time = str(timedelta(seconds = max_seconds)).split(':', 1)[1]

                    ## Setting remainder time
                                
                    remainder = str(timedelta(seconds = (total_seconds - max_seconds))).split(':', 1)[1]

                    shift['end_time'] = end_time

                    shift['shift_end'] = f'{end_time} / {remainder}'

            ## Setting goalie values

            if shift['position'] == 'G':

                shift['goalie'] = 1

            else:

                shift['goalie'] = 0

            ## Setting home and away values

            if shift['team_venue'] == 'HOME':

                shift['home'] = 1

                shift['away'] = 0

            else:

                shift['home'] = 0

                shift['away'] = 1

        ## Getting list of periods to iterate through

        periods = np.unique([x['period'] for x in game_list]).tolist()

        ## Setting list of teams to iterate through while iterating through the periods

        teams = ['HOME', 'AWAY']

        for period in periods:

            ## Getting max seconds for the period
            
            max_seconds = max([int(x['end_time_seconds']) for x in game_list if x['period'] == period])

            ## Iterating through home and away teams
            
            for team in teams:

                ## Getting the team's goalies for the game
                
                team_goalies = [x for x in game_list if x['goalie'] == 1 and x['team_venue'] == team]

                ## Getting the goalies for the period
                
                goalies = [x for x in game_list if x['goalie'] == 1 and x['team_venue'] == team and x['period'] == period]

                ## If there are no goalies changing during the period, we need to add them
                
                if len(goalies) < 1:
                    
                    if period == 1:

                        if len(team_goalies) < 1:

                            first_goalie = {}

                            starter = [x for x in actives if x['position'] == 'G' and x['team_venue'] == team and x['starter'] == 1][0]

                            new_values = {'season': season,
                                            'session': game_session,
                                            'game_id': game_id,
                                            'period': period,
                                            'team': starter['team'],
                                            'team_name': starter['team_name'],
                                            'team_venue': team,
                                            'player_name': starter['player_name'],
                                            'eh_id': starter['eh_id'],
                                            'team_jersey': starter['team_jersey'],
                                            'goalie': 1}

                            if team == 'HOME':

                                new_values.update({'home': 1,
                                                        'away': 0})

                            else:

                                new_values.update({'away': 1,
                                                        'home': 0})


                            first_goalie.update(new_values)
                            

                        first_goalie = team_goalies[0]

                        ## Initial dictionary is set using data from the first goalie to appear

                        goalie_shift = dict(first_goalie)

                    else:

                        ## Initial dictionary is set using data from the pervious goalie to appear

                        prev_goalie = [x for x in team_goalies if x['period'] == (period - 1)][-1]

                        goalie_shift = dict(prev_goalie)

                    ## Setting goalie shift number so we can identify later
                        
                    goalie_shift['number'] = 0

                    ## Setting the period for the current period
                        
                    goalie_shift['period'] = period

                    ## Setting the start time

                    goalie_shift['start_time'] = '0:00'

                    ## Setting the start time in seconds

                    goalie_shift['start_time_seconds'] = 0

                    ## If during regular time
                        
                    if period < 4:

                        ## Setting shift start value
                        
                        goalie_shift['shift_start'] = '0:00 / 20:00'

                        if max_seconds < 1200:

                            ## Setting end time value
                            
                            goalie_shift['end_time'] = '20:00'

                            ## Setting end time in seconds
                            
                            goalie_shift['end_time_seconds'] = 1200

                            ## Setting the duration, assuming they were out there the whole time
                            
                            goalie_shift['duration'] = '20:00'

                            ## Setting the duration in seconds, assuming they were out there the whole time
                            
                            goalie_shift['duration_seconds'] = 1200

                            ## Setting the shift end value

                            goalie_shift['shift_end'] = '20:00 / 0:00'

                    ## If the period is greater than 3
                        
                    else:

                        ## Need to account for whether regular season or playoffs
                        
                        if game_session != 'P':
                            
                            goalie_shift['shift_start'] = '0:00 / 5:00'
                            
                            total_seconds = 300
                            
                        elif game_session == 'P':
                            
                            goalie_shift['shift_start'] = '0:00 / 20:00'
                            
                            total_seconds = 1200

                        if max_seconds < total_seconds:

                            ## Getting end time
                            
                            end_time = str(timedelta(seconds = max_seconds)).split(':', 1)[1]

                            ## Getting remainder time
                                    
                            remainder = str(timedelta(seconds = (total_seconds - max_seconds))).split(':', 1)[1]

                            ## Setting values
                            
                            goalie_shift['end_time_seconds'] = max_seconds

                            goalie_shift['end_time'] = end_time

                            goalie_shift['shift_end'] = f'{end_time} / {remainder}'

                    ## Appending the new goalie shift to the game list
                        
                    game_list.append(goalie_shift)

            ## Iterating through the shifts
                    
            for shift in game_list:

                ## Fixing goalie errors 
                
                if shift['goalie'] == 1 and shift['period'] == period and shift['shift_end'] == '0:00 / 0:00':
                    
                    if period < 4:
                        
                        shift['shift_end'] = '20:00 / 0:00'
                        
                        shift['end_time'] = '20:00'
                        
                        shift['end_time_seconds'] = 1200
                        
                    else:
                        
                        if game_session == 'R':
                            
                            total_seconds = 300
                            
                        else:
                            
                            total_seconds = 1200
                            
                        end_time = str(timedelta(seconds = max_seconds)).split(':', 1)[1]
                                
                        remainder = str(timedelta(seconds = (total_seconds - max_seconds))).split(':', 1)[1]
                        
                        shift['end_time_seconds'] = max_seconds

                        shift['end_time'] = end_time

                        shift['shift_end'] = f'{end_time} / {remainder}'

        ## Sorting values

        game_list = sorted(game_list, key = lambda k: (k['away'], k['goalie'], k['jersey'], k['shift_count']))

        game_list = html_shifts_fixes(game_id, game_list)

        ## Adding the game data to the dictionary that will eventually be returned

        games_dict.update({game_id: game_list})

        ## Changing progress bar if this is the last game

        if game_id == game_ids[-1]:
            
            pbar.set_description(f'Finished scraping shifts data')

            ## Closing session object if function is not nested

            if nested == False:

                s.close()
            
        else:
        
            pbar.set_description(f'Finished scraping {game_id}')

        ## Adding current time to progress bar
        
        now = datetime.now()

        current_time = now.strftime("%H:%M:%S")

        postfix_str = f'{current_time}'
        
        pbar.set_postfix_str(postfix_str)

    ## Returning dataframe if not nested

    if nested == False:

        shifts_data = [shift for shifts in games_dict.values() for shift in shifts]

        df = pd.DataFrame(shifts_data)

        column_order = ['season', 'session', 'game_id', 'team', 'team_name', 'team_venue',
                        'player_name', 'eh_id', 'team_jersey', 'position', 'jersey',
                        'shift_count', 'period', 'start_time', 'end_time', 'duration',
                        'start_time_seconds', 'end_time_seconds', 'duration_seconds',
                        'shift_start', 'shift_end', 'goalie', 'home']

        column_order = [x for x in column_order if x in df.columns]

        df = df[column_order]

        return df

    ## Returning dictionary if nested

    else:

        return games_dict

############################################## HTML changes ##############################################

## [x]Refactored
## [x]Docstring
## []Comments
def scrape_changes(game_ids, roster_data = None, shifts_data = None, session = None, nested = True):
    
    '''

    --------- INFO ---------

    Scrapes the changes from the HTML shifts endpoint for a given game ID or list-like object of game IDs.
    Primarily used in combination with other scraping functions, but can be used standalone with nested parameter.

    By default returns a dictionary with game IDs as keys and lists of change-dictionaries as values.
    If nested is False, returns a Pandas DataFrame.

    Scrapes approximately 1-2 games per second. 

    --------- REQUIRED PARAMETER(S) ---------

    game_ids | integer or list-like object
        A single 10-digit API game ID (e.g., 2022020002) or list-like object of 10-digit game IDs (e.g., generator or Pandas Series)

    --------- OPTIONAL PARAMETER(S) ---------

    roster_data | dictionary: default = None
        If nested in other functions, can provide roster data from scrape function to prevent redundant hits to endpoints
        If standalone, will scrape roster data for each game automatically

    shifts_data | dictionary: default = None
        If nested in other functions, can provide shifts data from scrape function to prevent redundant hits to endpoints
        If standalone, will scrape shifts data for each game automatically

    session | requests Session object: default = None
        When using in another scrape function, can pass the requests session to improve speed

    nested | boolean: default = True
        If True, progress bar is disabled and returns a dictionary with game IDs as keys and lists of changes as dictionaries 
        If False, prints progress to the console and returns a dataframe

    --------- RETURNS ---------

    Default: Dictionary with game IDs as keys and lists of change-dictionaries as the values

    If nested = False, then returns a Pandas DataFrame, converting the dictionary keys to columns.

    Each change in each game list is a dictionary with the following fields and values:

        season: integer
            8-digit season code, e.g., 20222023

        session: object
            Regular season or playoffs, e.g., R

        game_id: integer
            10-digit game identifier, e.g., 2022020001

        event_team: object
            3-letter team code, e.g., NSH

        team_venue: object
            Whether team is home or away, e.g., home

        event: object
            Name of the event when including with other events, e.g., CHANGE

        event_type: object
            Type of the change when including with other events, e.g., HOME CHANGE
        
        description: object
            Description of the change, e.g., PLAYERS ON: FILIP FORSBERG, ALEX CARRIER, ROMAN JOSI, MIKAEL GRANLUND, JUUSE SAROS, MATT DUCHENE

        period: integer
            Game period, e.g., 1

        period_seconds: integer
            Period time in seconds (ascending), e.g., 0

        game_seconds: integer
            Game time in seconds (ascending), e.g., 0

        change_on_count: integer
            Number of players entering the ice, e.g., 6

        change_off_count: integer
            Number of players exiting the ice, e.g., 0

        change_on_jersey: list
            Abbreviations of players on, in jersey order, e.g., (NSH9, NSH45, NSH59, NSH64, NSH74, NSH95)
            If returning a DataFrame, will be a string, e.g., NSH9, NSH45, NSH59, NSH64, NSH74, NSH95

        change_on: list
            Names of players on, in jersey order, e.g., (FILIP FORSBERG, ALEX CARRIER, ROMAN JOSI, MIKAEL GRANLUND, JUUSE SAROS, MATT DUCHENE)
            If returning a DataFrame, will be a string, e.g., FILIP FORSBERG, ALEX CARRIER, ROMAN JOSI, MIKAEL GRANLUND, JUUSE SAROS, MATT DUCHENE

        change_on_id: list
            Evolving Hockey IDs of players on, in jersey order, e.g., (FILIP.FORSBERG, ALEX.CARRIER, ROMAN.JOSI, MIKAEL.GRANLUND, JUUSE.SAROS, MATT.DUCHENE)
            If returning a DataFrame, will be a string, e.g., FILIP.FORSBERG, ALEX.CARRIER, ROMAN.JOSI, MIKAEL.GRANLUND, JUUSE.SAROS, MATT.DUCHENE 

        change_on_positions: list
            Positions of players on, in jersey order, e.g., (L, D, D, C, G, C)
            If returning a DataFrame, will be a string, e.g., L, D, D, C, G, C

        change_off_jersey: list
            Abbreviations of players off, in jersey order
            If returning a DataFrame, will be a string

        change_off: list
            Names of players off, in jersey order
            If returning a DataFrame, will be a string

        change_off_id: list
            Evolving Hockey IDs of players off, in jersey order
            If returning a DataFrame, will be a string

        change_off_positions: list
            Positions of players off, in jersey order
            If returning a DataFrame, will be a string

        change_on_forwards_count: integer
            Number of forwards entering the ice, e.g., 6

        change_off_forwards_count: integer
            Number of forwards exiting the ice, e.g., 0

        change_on_forwards_jersey: list
            Abbreviations of forwards on, in jersey order, e.g., (NSH9, NSH64, NSH95)
            If returning a DataFrame, will be a string, e.g., NSH9, NSH64, NSH95

        change_on_forwards: list
            Names of forwards on, in jersey order, e.g., (FILIP FORSBERG, MIKAEL GRANLUND, MATT DUCHENE)
            If returning a DataFrame, will be a string, e.g., FILIP FORSBERG, MIKAEL GRANLUND, MATT DUCHENE

        change_on_forwards_id: list
            Evolving Hockey IDs of forwards on, in jersey order, e.g., (FILIP.FORSBERG, MIKAEL.GRANLUND, MATT.DUCHENE)
            If returning a DataFrame, will be a string, e.g., FILIP.FORSBERG, MIKAEL.GRANLUND, MATT.DUCHENE

        change_off_forwards_jersey: list
            Abbreviations of forwards off, in jersey order
            If returning a DataFrame, will be a string

        change_off_forwards: list
            Names of forwards off, in jersey order
            If returning a DataFrame, will be a string

        change_off_forwards_id: list
            Evolving Hockey IDs of forwards off, in jersey order
            If returning a DataFrame, will be a string

        change_on_defense_count: list
            Number of defense entering the ice, e.g., 6

        change_off_defense_count: list
            Number of defense exiting the ice, e.g., 0

        change_on_defense_jersey: list
            Abbreviations of defense on, in jersey order, e.g., (NSH45, NSH59)
            If returning a DataFrame, will be a string, e.g., NSH45, NSH59

        change_on_defense: list
            Names of defense on, in jersey order, e.g., (ALEX CARRIER, ROMAN JOSI)
            If returning a DataFrame, will be a string, e.g., ALEX CARRIER, ROMAN JOSI

        change_on_defense_id: list
            Evolving Hockey IDs of defense on, in jersey order, e.g., (ALEX.CARRIER, ROMAN.JOSI)
            If returning a DataFrame, will be a string, e.g., ALEX.CARRIER, ROMAN.JOSI

        change_off_defense_jersey: list
            Abbreviations of defense off, in jersey order
            If returning a DataFrame, will be a string

        change_off_defense: list
            Names of defense off, in jersey order
            If returning a DataFrame, will be a string

        change_off_defense_id: list
            Evolving Hockey IDs of defense off, in jersey order
            If returning a DataFrame, will be a string

        change_on_goalie_count: integer
            Number of goalies entering the ice, e.g., 6

        change_off_goalie_count: integer
            Number of goalies exiting the ice, e.g., 0

        change_on_goalie_jersey: list
            Abbreviations of goalies on, in jersey order, e.g., (NSH74)
            If returning a DataFrame, will be a string, e.g., NSH74

        change_on_goalie: list
            Names of goalies on, in jersey order, e.g., (JUUSE SAROS)
            If returning a DataFrame, will be a string, e.g., JUUSE SAROS

        change_on_goalie_id: list
            Evolving Hockey IDs of goalies on, in jersey order, e.g., (JUUSE.SAROS)            
            If returning a DataFrame, will be a string, e.g., JUUSE.SAROS

        change_off_goalie_jersey: list
            Abbreviations of goalies off, in jersey order
            If returning a DataFrame, will be a string

        change_off_goalie: list
            Names of goalies off, in jersey order
            If returning a DataFrame, will be a string

        change_off_goalie_id: list
            Evolving Hockey IDs of players off, in jersey order
            If returning a DataFrame, will be a string


    '''
    
    ## Convert game IDs to list if given a single game ID
    game_ids = convert_to_list(obj = game_ids, object_type = 'game ID')
    
    number_of_games = len(game_ids)
    
    ## Important lists
    games_dict = {}
    
    if session == None:
    
        s = s_session()
        
    else:
        
        s = session
        
    pbar = tqdm(game_ids, disable = nested)
    
    for game_id in pbar:
            
        if shifts_data is None:
            
            shifts = scrape_shifts(game_id, roster_data = roster_data, session = s, nested = True)[game_id]
            
        else:
            
            shifts = shifts_data[game_id].copy()

        season, game_session = game_id_info(game_id)

        game_list = []

        periods = np.unique([x['period'] for x in shifts]).tolist()

        teams = ['HOME', 'AWAY'] 

        for period in periods:
            
            max_seconds = max([x['end_time_seconds'] for x in shifts if x['period'] == period])
            
            for team in teams:
                
                changes_dict = {}
            
                changes_on = np.unique([x['start_time_seconds'] for x in shifts
                                        if x['period'] == period
                                        and x['team_venue'] == team
                                       ]).tolist()
                
                for change_on in changes_on:

                    players_on = [x for x in shifts
                                  if x['period'] == period
                                  and x['start_time_seconds'] == change_on
                                  and x['team_venue'] == team]

                    players_on = sorted(players_on, key = lambda k: (k['jersey']))

                    f_positions = ['L', 'C', 'R']

                    forwards_on = [x for x in shifts if x['period'] == period
                                    and x['start_time_seconds'] == change_on
                                    and x['team_venue'] == team
                                    and x['position'] in f_positions]

                    forwards_on = sorted(forwards_on, key = lambda k: (k['jersey']))

                    defense_on = [x for x in shifts
                                  if x['period'] == period
                                  and x['start_time_seconds'] == change_on
                                  and x['team_venue'] == team
                                  and x['position'] == 'D']

                    defense_on = sorted(defense_on, key = lambda k: (k['jersey']))

                    goalies_on = [x for x in shifts
                                  if x['period'] == period
                                  and x['start_time_seconds'] == change_on
                                  and x['team_venue'] == team
                                  and x['position'] == 'G']
                    
                    goalies_on = sorted(goalies_on, key = lambda k: (k['jersey']))

                    new_values = {'season': season,
                                  'session': game_session,
                                  'game_id': game_id,
                                  'event': 'CHANGE',
                                  'event_team': players_on[0]['team'],
                                  'home': players_on[0]['home'],
                                  'away': players_on[0]['away'],
                                  'team_venue': team,
                                  'period': period,
                                  'period_time': players_on[0]['start_time'],
                                  'period_seconds': players_on[0]['start_time_seconds'],
                                  'change_on_count': len(players_on),
                                  'change_off_count': 0,
                                  'change_on_jersey': [x['team_jersey'] for x in players_on],
                                  'change_on': [x['player_name'] for x in players_on],
                                  'change_on_id': [x['eh_id'] for x in players_on],
                                  'change_on_positions': [x['position'] for x in players_on],
                                  'change_off_jersey': '',
                                  'change_off': '',
                                  'change_off_id': '',
                                  'change_off_positions': '',
                                  'change_on_forwards_count': len(forwards_on),
                                  'change_off_forwards_count': 0,
                                  'change_on_forwards_jersey': [x['team_jersey'] for x in forwards_on],
                                  'change_on_forwards': [x['player_name'] for x in forwards_on],
                                  'change_on_forwards_id': [x['eh_id'] for x in forwards_on],
                                  'change_off_forwards_jersey': '',
                                  'change_off_forwards': '',
                                  'change_off_forwards_id': '',
                                  'change_on_defense_count': len(defense_on),
                                  'change_off_defense_count': 0,
                                  'change_on_defense_jersey': [x['team_jersey'] for x in defense_on],
                                  'change_on_defense': [x['player_name'] for x in defense_on],
                                  'change_on_defense_id': [x['eh_id'] for x in defense_on],
                                  'change_off_defense_jersey': '',
                                  'change_off_defense': '',
                                  'change_off_defense_id': '',
                                  'change_on_goalie_count': len(goalies_on),
                                  'change_off_goalie_count': 0,
                                  'change_on_goalie_jersey': [x['team_jersey'] for x in goalies_on],
                                  'change_on_goalie': [x['player_name'] for x in goalies_on],
                                  'change_on_goalie_id': [x['eh_id'] for x in goalies_on],
                                  'change_off_goalie_jersey': '',
                                  'change_off_goalie': '',
                                  'change_off_goalie_id': '',
                                 }
                    
                    changes_dict.update({change_on: new_values})
                    
                changes_off = np.unique([x['end_time_seconds'] for x in shifts
                                         if x['period'] == period
                                         and x['team_venue'] == team
                                       ]).tolist()
                
                for change_off in changes_off:
                    
                    players_off = [x for x in shifts
                                   if x['period'] == period
                                   and x['end_time_seconds'] == change_off
                                   and x['team_venue'] == team]
                    
                    players_off = sorted(players_off, key = lambda k: (k['jersey']))

                    f_positions = ['L', 'C', 'R']

                    forwards_off = [x for x in shifts if x['period'] == period
                                    and x['end_time_seconds'] == change_off
                                    and x['team_venue'] == team
                                    and x['position'] in f_positions]

                    forwards_off = sorted(forwards_off, key = lambda k: (k['jersey']))

                    defense_off = [x for x in shifts
                                  if x['period'] == period
                                  and x['end_time_seconds'] == change_off
                                  and x['team_venue'] == team
                                  and x['position'] == 'D']

                    defense_off = sorted(defense_off, key = lambda k: (k['jersey']))

                    goalies_off = [x for x in shifts
                                  if x['period'] == period
                                  and x['end_time_seconds'] == change_off
                                  and x['team_venue'] == team
                                  and x['position'] == 'G']
                    
                    goalies_off = sorted(goalies_off, key = lambda k: (k['jersey']))

                    new_values = {'season': season,
                                  'session': game_session,
                                  'game_id': game_id,
                                  'event': 'CHANGE',
                                  'event_team': players_on[0]['team'],
                                  'event_team': players_off[0]['team'],
                                  'team_venue': team,
                                  'home': players_off[0]['home'],
                                  'away': players_off[0]['away'],
                                  'period': period,
                                  'period_time': players_off[0]['end_time'],
                                  'period_seconds': players_off[0]['end_time_seconds'],
                                  'change_off_count': len(players_off),
                                  'change_off_jersey': [x['team_jersey'] for x in players_off],
                                  'change_off': [x['player_name'] for x in players_off],
                                  'change_off_id': [x['eh_id'] for x in players_off],
                                  'change_off_positions': [x['position'] for x in players_off],
                                  'change_off_forwards_count': len(forwards_off),
                                  'change_off_forwards_jersey': [x['team_jersey'] for x in forwards_off],
                                  'change_off_forwards': [x['player_name'] for x in forwards_off],
                                  'change_off_forwards_id': [x['eh_id'] for x in forwards_off],
                                  'change_off_defense_count': len(defense_off),
                                  'change_off_defense_jersey': [x['team_jersey'] for x in defense_off],
                                  'change_off_defense': [x['player_name'] for x in defense_off],
                                  'change_off_defense_id': [x['eh_id'] for x in defense_off],
                                  'change_off_goalie_count': len(goalies_off),
                                  'change_off_goalie_jersey': [x['team_jersey'] for x in goalies_off],
                                  'change_off_goalie': [x['player_name'] for x in goalies_off],
                                  'change_off_goalie_id': [x['eh_id'] for x in goalies_off],
                                 }
                    
                    if change_off in changes_on:
                    
                        changes_dict[change_off].update(new_values)
                        
                    else:

                        new_values.update({'change_on_count': 0,
                                            'change_on_forwards_count': 0,
                                            'change_on_defense_count': 0,
                                            'change_on_goalie_count': 0})
                        
                        changes_dict[change_off] = new_values
                    
                game_list.extend(list(changes_dict.values()))

        game_list = sorted(game_list, key = lambda k: (k['period'], k['period_seconds'], k['away']))

        for change in game_list:

            players_on = ', '.join(change.get('change_on', []))

            players_off = ', '.join(change.get('change_off', []))

            on_num = len(change.get('change_on', []))

            off_num = len(change.get('change_off', []))

            if  on_num > 0 and off_num > 0:

                change['description'] = f"PLAYERS ON: {players_on} / PLAYERS OFF: {players_off}"

            if on_num > 0 and off_num == 0:

                 change['description'] = f"PLAYERS ON: {players_on}"

            if off_num > 0 and on_num == 0:

                change['description'] = f"PLAYERS OFF: {players_off}"

            if change['period'] == 5 and game_session == 'R':

                change['game_seconds'] = 3900 + change['period_seconds']

            else:

                change['game_seconds'] = (int(change['period']) - 1) * 1200 + change['period_seconds']

            if change['home'] == 1:

                change['event_type'] = 'HOME CHANGE'

            else:

                change['event_type'] = 'AWAY CHANGE'
            
        games_dict.update({game_id: game_list})

        if game_id == game_ids[-1]:
            
            pbar.set_description(f'Finished scraping changes data')

            if nested == False:

                s.close()
            
        else:
        
            pbar.set_description(f'Finished scraping {game_id}')
        
        now = datetime.now()

        current_time = now.strftime("%H:%M:%S")

        postfix_str = f'{current_time}'
        
        pbar.set_postfix_str(postfix_str)

    if nested == False:

        changes_data = [change for changes in games_dict.values() for change in changes]

        list_fields = ['change_on_jersey', 'change_on', 'change_on_id', 'change_on_positions',
                        'change_off_jersey', 'change_off', 'change_off_id', 'change_off_positions',
                        'change_on_forwards_jersey',
                        'change_on_forwards', 'change_on_forwards_id', 'change_off_forwards_jersey',
                        'change_off_forwards', 'change_off_forwards_id', 'change_on_defense_jersey',
                        'change_on_defense',
                        'change_on_defense_id', 'change_off_defense_jersey', 'change_off_defense',
                        'change_off_defense_id', 
                        'change_on_goalie_jersey', 'change_on_goalie', 'change_on_goalie_id',
                        'change_off_goalie_jersey', 'change_off_goalie', 'change_off_goalie_id',
                        ]

        for change in changes_data:

            for list_field in list_fields:

                change[list_field] = ', '.join(change.get(list_field, ''))

        df = pd.DataFrame(changes_data)

        column_order = ['season', 'session', 'game_id', 'event_team', 'event_team_name', 'team_venue',
                        'event', 'event_type', 'description',
                        'period', 'period_seconds', 'game_seconds', 'change_on_count', 'change_off_count',
                        'change_on_jersey', 'change_on', 'change_on_id', 'change_on_positions',
                        'change_off_jersey', 'change_off', 'change_off_id', 'change_off_positions',
                        'change_on_forwards_count', 'change_off_forwards_count', 'change_on_forwards_jersey',
                        'change_on_forwards', 'change_on_forwards_id', 'change_off_forwards_jersey',
                        'change_off_forwards', 'change_off_forwards_id', 'change_on_defense_count',
                        'change_off_defense_count', 'change_on_defense_jersey', 'change_on_defense',
                        'change_on_defense_id', 'change_off_defense_jersey', 'change_off_defense',
                        'change_off_defense_id', 'change_on_goalie_count', 'change_off_goalie_count',
                        'change_on_goalie_jersey', 'change_on_goalie', 'change_on_goalie_id',
                        'change_off_goalie_jersey', 'change_off_goalie', 'change_off_goalie_id',
                        ]

        column_order = [x for x in column_order if x in df.columns]

        df = df[column_order]

        df = df.replace('', np.nan)

        return df

    else:

        return games_dict

############################################## API events ##############################################

## [x]Refactored
## []Docstring
## []Comments
def scrape_api_events(game_ids, live_response = None, session = None, nested = True):
    
    '''

    --------- INFO ---------

    Scrapes the event data from the NHL API for a given game ID or list-like object of game IDs.
    Primarily used in combination with other scraping functions, but can be used standalone with nested parameter.
    Data do not exist before 2010-2011 season.

    By default returns a dictionary with game IDs as keys and lists of change-dictionaries as values.
    If nested is False, returns a Pandas DataFrame.

    Scrapes approximately x-y games per second. 

    --------- REQUIRED PARAMETER(S) ---------

    game_ids | integer or list-like object
        A single 10-digit API game ID (e.g., 2019020684) or list-like object of 10-digit game IDs (e.g., generator or Pandas Series)

    --------- OPTIONAL PARAMETER(S) ---------

    live_response | JSON object: default = None
        When using in another scrape function, can pass the live endpoint response as a JSON object to prevent redundant hits

    session | requests Session object: default = None
        When using in another scrape function, can pass the requests session to improve speed

    nested | boolean: default = True
        If True, progress bar is disabled and returns a dictionary with game IDs as keys and lists of changes as dictionaries 
        If False, prints progress to the console and returns a dataframe

    --------- RETURNS ---------

    Default: Dictionary with game IDs as keys and lists of event-dictionaries as the values

    If nested = False, then returns a Pandas DataFrame, converting the dictionary keys to columns.

    Each change in each game list is a dictionary with the following fields and values:

        season: integer
            8-digit season code, e.g., 20192020

        session: object
            Regular season or playoffs, e.g., R

        game_id: integer
            10-digit game identifier, e.g., 2019020684

        game_date: object
            Date of game in Eastern time-zone, e.g., 2020-01-09

        event_team: object
            3-letter abbreviation of the team for the event, e.g., NSH 

        event_team_name: object
            Name of the team for the event, e.g., NASHVILLE PREDATORS

        event_idx: object
            Unique index number of event, in chronological order, e.g., 333

        period: integer
            Period number, e.g., 3

        period_seconds: integer
            Number of seconds elapsed in the period, e.g., 1178

        game_seconds: integer
            Number of seconds elapsed in the game, e.g., 3578

        event: object
            Name of the event, e.g., GOAL

        event_type: object
            Type of the event, e.g., GOAL

        description: object
            Description of the event, e.g., PEKKA RINNE (1) WRIST SHOT, ASSISTS: NONE

        coords_x: float
            X coordinates of event, e.g., -96

        coords_y: float
            Y coordinates of event, e.g., 11

        home_score: integer
            Number of goals scored by home team, e.g., 2

        away_score: integer
            Number of goals scored by away team, e.g., 5

        player_1: object
            Name of the player, e.g.,  PEKKA RINNE

        player_1_api_id: integer
            Unique ID for the player, e.g., 8471469

        player_1_eh_id: object
            Identifier that can be used to match with Evolving Hockey data, e.g., PEKKA.RINNE

        player_1_type: object
            Type of player, e.g., SHOOTER
        
        player_2: object
            Name of the player

        player_2_api_id: integer
            Unique ID for the player

        player_2_eh_id: object
            Identifier that can be used to match with Evolving Hockey data

        player_2_type: object
            Type of player

        player_3: object
            Name of the player

        player_3_api_id: object
            Unique ID for the player

        player_3_eh_id: object
            Identifier that can be used to match with Evolving Hockey data

        player_3_type: object
            Type of player

        player_4: object
            Name of the player

        player_4_api_id: object
            Unique ID for the player

        player_4_eh_id: object
            Identifier that can be used to match with Evolving Hockey data

        player_4_type: object
            Type of player

        game_winning_goal: integer
            Whether shot is game winning goal, e.g., 0

        empty_net_goal: integer
            Whether shot is an empty net goal, e.g., 1

        penalty_severity: object
            Whether penalty is a minor or major

        penalty_minutes: float
            Length of penalty in minutes

        event_dt: object
            Datetime in eastern time zone that event occurred, e.g., Timestamp('2020-01-09 23:01:47-0500', tz='US/Eastern')

        time_elapsed: object
            Time delta object for amount of time elapsed since game start, e.g., 0 days 02:21:37

        time_elapsed_seconds: float
            Time elapsed since game start in seconds, e.g., 8497.0

        version: integer
            Used for matching with HTML events, e.g., 1

    '''
    
    ## Convert game IDs to list if given a single game ID
    game_ids = convert_to_list(obj = game_ids, object_type = 'game ID')
    
    ## Important lists
    games_dict = {}
        
    if session == None:
    
        s = s_session()
    
    else:
        
        s = session
    
    pbar = tqdm(game_ids, disable = nested)
    
    for game_id in pbar:

        year = int(str(game_id)[0:4])

        season, game_session = game_id_info(game_id)
        
        if (str(game_id).isdigit() == False
            or len(str(game_id)) != 10
            or year < 2010
            or game_session not in ['R', 'P']):
            
            pbar.set_description(f'{game_id} not a valid game_id')

            now = datetime.now()

            current_time = now.strftime("%H:%M:%S")

            postfix_str = f'{current_time}'

            pbar.set_postfix_str(postfix_str)
            
            continue
            
        if live_response == None:
        
            response = s.get(f'https://statsapi.web.nhl.com/api/v1/game/{game_id}/feed/live').json()
            
        else:
            
            response = live_response

        rosters = scrape_api_rosters(game_id, live_response = response, session = s, nested = True)[game_id]

        rosters = {x['api_id']: x for x in rosters}

        game_info = scrape_game_info(game_id, live_response = response, session = s, nested = True)[game_id]
        
        plays = response['liveData']['plays']['allPlays']

        if plays == []:
            
            pbar.set_description(f'{game_id} not available')

            now = datetime.now()

            current_time = now.strftime("%H:%M:%S")

            postfix_str = f'{current_time}'

            pbar.set_postfix_str(postfix_str)
            
            continue

        game_list = []
            
        for play in plays:
                    
            result = play['result']

            about = play['about']
            
            time_split = about['periodTime'].split(':', 1)

            coordinates = play['coordinates']
            
            event_team = play.get('team', {})
            
            if event_team.get('name') == 'PHOENIX COYOTES':
                
                event_team['name'] = 'ARIZONA COYOTES'
                
                event_team['triCode'] = 'ARI'
                
            if 'CANADIENS' in event_team.get('name', ''):
                
                event_team['name'] = 'MONTREAL CANADIENS'
                
            if event_team.get('triCode') == game_info['home_team']:
                
                home = 1
                
            elif event_team.get('triCode') == game_info['away_team']:
                
                home = 0
                
            else:
                
                home = np.nan
            
            event_types = {'BLOCKED_SHOT': 'BLOCK', 'BLOCKEDSHOT': 'BLOCK', 'MISSED_SHOT': 'MISS', 'FACEOFF': 'FAC',
                           'PENALTY': 'PENL', 'GIVEAWAY': 'GIVE', 'TAKEAWAY': 'TAKE', 'MISSEDSHOT': 'MISS', 
                           'PERIOD_START': 'PSTR', 'PERIOD_END': 'PEND', 'PERIOD_OFFICIAL': 'POFF', 'GAME_OFFICIAL': 'GOFF',
                           'GAME_SCHEDULED': 'GSCH', 'GAME_END': 'GEND', 'CHALLENGE': 'CHL', 'SHOOTOUT_COMPLETE': 'SOC',
                           'EARLY_INT_START': 'EISTR', 'EARLY_INT_END': 'EIEND', 'PERIOD_READY': 'PREADY',
                           'FAILED_SHOT_ATTEMPT': 'MISS', 'EMERGENCY_GOALTENDER': 'EGT',
                           }
            
            new_values = {'season': season,
                          'session': game_session,
                          'game_id': game_id,
                          'game_date': game_info['game_date'],
                          'event_idx': int(about['eventIdx']),
                          'period': int(about['period']),
                          'period_time': about['periodTime'],
                          'period_seconds': (int(time_split[0]) * 60) + int(time_split[1]),
                          'event': event_types.get(result['eventTypeId'], result['eventTypeId']),
                          'event_type': result['event'].upper(),
                          'event_code': result['eventCode'],
                          'description': result['description'].upper(),
                          'event_team': event_team.get('triCode', ''), 
                          'event_team_name': unidecode(event_team.get('name', '').upper()),
                          'coords_x': float(coordinates.get('x', np.nan)),
                          'coords_y': float(coordinates.get('y', np.nan)),
                          'home': home,
                          'home_score': int(about['goals']['home']),
                          'away_score': int(about['goals']['away']),
                          'event_detail': result.get('secondaryType', '').upper(),
                          'strength_code': result.get('strength_code', ''),
                          'strength_name': result.get('strength_name', ''),
                          'game_winning_goal': int(result.get('gameWinningGoal', 0)),
                          'empty_net_goal': int(result.get('emptyNet', 0)), 
                          'penalty_severity': result.get('penaltySeverity', '').upper(),
                          'penalty_minutes': float(result.get('penaltyMinutes', np.nan)),
                          'event_dt': pd.to_datetime(about['dateTime']).tz_convert('US/Eastern'),
                          
                         }
            
            play.update(new_values)
            
            if about['periodType'] != 'SHOOTOUT':
                
                play['game_seconds'] = ((play['period'] - 1) * 1200) + play['period_seconds']
                
            else:
                
                play['game_seconds'] = 3900 + play['period_seconds']
            
            players = play.get('players')
            
            if players is not None:
                
                if play['event'] == 'BLOCK' and players[1]['playerType'].upper() == 'BLOCKER':
                    
                    players[0], players[1] = players[1], players[0]
                    
                    if play['event_team'] == game_info['home_team']:

                        play['event_team'] = game_info['away_team']

                        play['event_team_name'] = game_info['away_team_name']
                        
                    else:
                        
                        play['event_team'] = game_info['home_team']

                        play['event_team_name'] = game_info['home_team_name']
                        
                if play['event'] == 'FAC' and players[1]['playerType'].upper() == 'WINNER':
                    
                    players[0], players[1] = players[1], players[0]
                    
                    if play['event_team'] == game_info['home_team']:

                        play['event_team'] = game_info['away_team']

                        play['event_team_name'] = game_info['away_team_name']
                        
                    else:
                        
                        play['event_team'] = game_info['home_team']

                        play['event_team_name'] = game_info['home_team_name']
                        
                if (play['event'] == 'PENL'):

                    if ('served by' in play['description'].lower() and
                        ('too many' in play['description'].lower() or
                            'team' in play['description'].lower() or
                            'unsucc. chlg' in play['description'].lower() or
                            'bench' in play['description'].lower() or
                            play['description'].lower()[:5] == 'abuse' or
                            'head coach' in play['description'].lower()
                            or 'un. chlg' in play['description'].lower()
                            )):

                        players.insert(0, {'player': {'fullName': 'BENCH',
                                                        'id': 'BENCH',},
                                            'playerType': 'PENALTYON'})
                        
                        players[1]['playerType'] = 'SERVED BY'

                    elif 'against' in play['description'].lower() and 'served by' in play['description'].lower():

                        if players[1]['playerType'].upper() == 'SERVEDBY':

                            players[1], players[2] = players[2], players[1]

                    if len(players) > 2:

                        if (players[0]['player']['id'] == players[2]['player']['id'] and 
                            players[0]['playerType'].upper() == 'PENALTYON' and
                            players[2]['playerType'].upper() == 'SERVEDBY'):

                            players.pop(2)
                    
                for idx, player in enumerate(players):

                    num = idx + 1

                    player_data = player['player']

                    play[f'player_{num}'] = unidecode(player_data['fullName'].upper())

                    play[f'player_{num}'] = play[f'player_{num}'].replace('ALEXANDRE', 'ALEX').replace('ALEXANDER', 'ALEX').replace('CHRISTOPHER', 'CHRIS')

                    play[f'player_{num}'] = correct_names_dict.get(play[f'player_{num}'], play[f'player_{num}'])

                    play[f'player_{num}_api_id'] = player_data['id']

                    if play[f'player_{num}'] != 'BENCH':

                        name_split = play[f'player_{num}'].split(' ', 1)

                        play[f'player_{num}_eh_id'] = f'{name_split[0].strip()}.{name_split[1].strip()}'.replace('..', '.')

                        play[f'player_{num}_eh_id'] = correct_api_names_dict.get(play[f'player_{num}_api_id'], play[f'player_{num}_eh_id'])

                        play[f'player_{num}_age'] = rosters.get(player_data['id'], {}).get('age', '')

                        if 'shoots' in rosters.get(player_data['id'], {}).keys():

                            play[f'player_{num}_hand'] = rosters.get(player_data['id'], {}).get('shoots', '')

                        elif 'catches' in rosters.get(player_data['id'], {}).keys():

                            play[f'player_{num}_hand'] = rosters.get(player_data['id'], {}).get('catches', '')

                    else:

                        play[f'player_{num}_eh_id'] = 'BENCH'

                        play[f'player_{num}_age'] = ''

                        play[f'player_{num}_hand'] = ''
                    
                    play[f'player_{num}_type'] = player['playerType'].upper()
                
            delete_list = ['result', 'about', 'coordinates', 'team', 'players']
            
            
            if play['event'] == 'PSTR' and play['period'] == 1:
                
                game_start = play['event_dt']
            
            play = {k: v for k, v in play.items() if k not in delete_list}
            
            game_list.append(play)

        game_list = sorted(game_list, key = lambda k: (k['event_idx']))
            
        for event in game_list:
            
            time_elapsed = pd.to_datetime(event['event_dt']) - game_start
            
            event['time_elapsed'] = str(time_elapsed)
                    
            event['time_elapsed_seconds'] = time_elapsed.total_seconds()
            
            if 'version' in event.keys():
                
                continue
            
            other_events = [x for x in game_list
                            if x != event
                            and x['event'] == event['event']
                            and x['game_seconds'] == event['game_seconds']
                            and x.get('player_1_api_id') is not None
                            and x['period'] == event['period']
                            and event.get('player_1_api_id') is not None
                            and x['player_1_api_id'] == event['player_1_api_id']
                         ]

            version = 1
            
            event['version'] = 1

            if len(other_events) > 0:
                
                for idx, other_event in enumerate(other_events):

                    if 'version' not in other_event.keys():

                        version += 1
                    
                        other_event['version'] = version

        game_list = api_events_fixes(game_id, game_list)
    
        games_dict.update({game_id: game_list})  
        
        if game_id == game_ids[-1]:
            
            pbar.set_description(f'Finished scraping events data from the NHL API')

            if nested == False:

                s.close()
            
        else:
        
            pbar.set_description(f'Finished scraping {game_id}')
        
        now = datetime.now()

        current_time = now.strftime("%H:%M:%S")

        postfix_str = f'{current_time}'
        
        pbar.set_postfix_str(postfix_str)

    if nested == False:

        events_data = [event for events in games_dict.values() for event in events]

        df = pd.DataFrame(events_data)

        columns = ['season', 'session', 'game_id', 'game_date', 'event_team', 'event_team_name',
                       'event_idx', 'period', 'period_seconds', 'game_seconds',
                       'event', 'event_type',  'description', 'coords_x', 'coords_y',
                       'home_score', 'away_score', 'player_1', 'player_1_api_id', 'player_1_eh_id',
                       'player_1_type', 'player_1_age', 'player_1_hand', 'player_2', 'player_2_api_id', 'player_2_eh_id',
                       'player_2_type', 'player_2_age', 'player_2_hand',
                       'player_3', 'player_3_api_id', 'player_3_eh_id', 'player_3_type', 'player_3_age', 'player_3_hand', 'player_4',
                       'player_4_api_id', 'player_4_eh_id', 'player_4_type', 'player_4_age', 'player_4_hand', 'game_winning_goal',
                       'empty_net_goal', 'penalty_severity', 'penalty_minutes', 'event_dt', 'time_elapsed',
                       'time_elapsed_seconds', 'version']

        column_order = [x for x in columns if x in df.columns]

        df = df[column_order]

        df = df.replace('', np.nan)

        return df

    else:

        return games_dict
    
############################################## HTML events ##############################################

## []Refactored
## [x]Docstring
## []Comments
def scrape_html_events(game_ids, roster_data = None, session = None, nested = True):
    
    '''
    --------- INFO ---------

    Scrapes the event data from the NHL API for a given game ID or list-like object of game IDs.
    Primarily used in combination with other scraping functions, but can be used standalone with nested parameter.
    Data do not exist before 2010-2011 season.

    By default returns a dictionary with game IDs as keys and lists of change-dictionaries as values.
    If nested is False, returns a Pandas DataFrame.

    Scrapes approximately x-y games per second. 

    --------- REQUIRED PARAMETER(S) ---------

    game_ids | integer or list-like object
        A single 10-digit API game ID (e.g., 2019020684) or list-like object of 10-digit game IDs (e.g., generator or Pandas Series)

    --------- OPTIONAL PARAMETER(S) ---------

    roster_data | dictionary: default = None
        If nested in other functions, can provide roster data from scrape function to prevent redundant hits to endpoints
        If standalone, will scrape roster data for each game automatically

    session | requests Session object: default = None
        When using in another scrape function, can pass the requests session to improve speed

    nested | boolean: default = True
        If True, progress bar is disabled and returns a dictionary with game IDs as keys and lists of changes as dictionaries 
        If False, prints progress to the console and returns a dataframe

    --------- RETURNS ---------

    Default: Dictionary with game IDs as keys and lists of event-dictionaries as the values

    If nested = False, then returns a Pandas DataFrame, converting the dictionary keys to columns.

    Each change in each game list is a dictionary with the following fields and values:

        season: integer
            8-digit season code, e.g., 20192020

        session: object
            Regular season or playoffs, e.g., R

        game_id: integer
            10-digit game identifier, e.g., 2019020684

        event_team: object
            3-letter abbreviation of the team for the event, e.g., NSH 

        event_idx: object
            Unique index number of event, in chronological order, e.g., 333

        period: integer
            Period number, e.g., 3

        period_seconds: integer
            Number of seconds elapsed in the period, e.g., 1178

        game_seconds: integer
            Number of seconds elapsed in the game, e.g., 3578

        event: object
            Name of the event, e.g., GOAL

        description: object
            Description of the event, e.g., NSH #35 RINNE(1), WRIST, DEF. ZONE, 185 FT.

        strength: object
            Strength state for event, e.g., EV

        zone: object
            Zone location of event, e.g., DEF

        player_1: object
            Name of the player, e.g.,  PEKKA RINNE

        player_1_eh_id: object
            Identifier that can be used to match with Evolving Hockey data, e.g., PEKKA.RINNE
        
        player_2: object
            Name of the player

        player_2_eh_id: object
            Identifier that can be used to match with Evolving Hockey data

        player_3: object
            Name of the player

        player_3_eh_id: object
            Identifier that can be used to match with Evolving Hockey data

        pbp_distance: float
            Shot distance from net in feet, e.g., 185

        shot_type: object
            Type of shot, e.g., WRIST

        penalty: object
            Name of penalty

        penalty_length: object
            Length of penalty in minutes

        version: integer
            Used for matching with HTML events, e.g., 1

    ''' 
    
    ## IMPORTANT LISTS AND DICTIONARIES
    NEW_TEAM_NAMES = {'L.A': 'LAK', 'N.J': 'NJD', 'S.J': 'SJS', 'T.B': 'TBL', 'PHX': 'ARI'}

    ## Compiling regex expressions to save time later
        
    event_team_re = re.compile('^([A-Z]{3}|[A-Z]\.[A-Z])')
    numbers_re = re.compile('#([0-9]{1,2})')
    event_players_re = re.compile('([A-Z]{3}\s+\#[0-9]{1,2})')
    positions_re = re.compile('([A-Z]{1,2})')
    fo_team_re = re.compile('([A-Z]{3}) WON')
    block_team_re = re.compile('BLOCKED BY\s+([A-Z]{3})')
    skaters_re = re.compile(r'(\d+)')
    zone_re = re.compile(r'([A-Za-z]{3}). ZONE')
    penalty_re = re.compile('([A-Za-z]*|[A-Za-z]*-[A-Za-z]*|[A-Za-z]*\s+\(.*\))\s*\(')
    penalty_length_re = re.compile('(\d+) MIN')
    shot_re = re.compile(',\s+([A-za-z]*|[A-za-z]*-[A-za-z]*),')
    distance_re = re.compile('(\d+) FT')
    served_re = re.compile('([A-Z]{3})\s.+SERVED BY: #([0-9]+)')
    #served_drawn_re = re.compile('([A-Z]{3})\s#.*\sSERVED BY: #([0-9]+)')
    drawn_re = re.compile('DRAWN BY: ([A-Z]{3}) #([0-9]+)')
    
    ## Convert game IDs to list if given a single game ID
    game_ids = convert_to_list(obj = game_ids, object_type = 'game ID')
    
    if session == None:
    
        s = s_session()
        
    else:
        
        s = session

    games_dict = {}
        
    pbar = tqdm(game_ids, disable = nested)

    for game_id in pbar:
        
        season, game_session, html_id = game_id_info(game_id, html_id = True)
            
        if roster_data is None:
            
            roster = scrape_html_rosters(game_id, session = s, nested = True)[game_id]
            
        else:
            
            roster = roster_data[game_id].copy()

        actives = {player['team_jersey']: player for player in roster if player['status'] == 'ACTIVE'}

        scratches = {player['team_jersey']: player for player in roster if player['status'] == 'SCRATCH'}

        url = f'http://www.nhl.com/scores/htmlreports/{season}/PL{html_id}.HTM'

        response = s.get(url)

        soup = BeautifulSoup(response.content.decode('ISO-8859-1'), 'lxml')
        
        events = []

        if soup.find('html') is None:

            pbar.set_description(f'{game_id} not a valid game_id')

            now = datetime.now()

            current_time = now.strftime("%H:%M:%S")

            postfix_str = f'{current_time}'

            pbar.set_postfix_str(postfix_str)
            
            continue 

        tds = soup.find_all("td", {"class": re.compile('.*bborder.*')})

        events_data = hs_strip_html(tds)

        events_data = [unidecode(x).replace('\n ', ', ').replace('\n', '') for x in events_data]

        length = int(len(events_data) / 8)

        events_data = np.array(events_data).reshape(length, 8)
        
        for idx, event in enumerate(events_data):
                
            column_names = ['event_idx', 'period', 'strength', 'time', 'event', 'description', 'away_skaters', 'home_skaters']

            if '#' in event:

                continue

            else:

                event = dict(zip(column_names, event))
                
                new_values = {'season': season,
                              'session': game_session,
                              'game_id': game_id,
                              'event_idx': int(event['event_idx']),
                              'description': unidecode(event['description']).upper(),
                              'period': event['period'],
                         }
            
                event.update(new_values)

                ## This event is missing from the API and doesn't have a player in the HTML endpoint

                if game_id == 2022020194 and event['event_idx'] == 134:

                    continue

                if game_id == 2022020673 and event['event_idx'] == 208:

                    continue

                events.append(event)

        for event in events:

            non_descripts = {'PGSTR': 'PRE-GAME START',
                                'PGEND': 'PRE-GAME END',
                                'ANTHEM': 'NATIONAL ANTHEM',
                                'EISTR': 'EARLY INTERMISSION START',
                                'EIEND': 'EARLY INTERMISSION END'}

            if event['event'] in list(non_descripts.keys()):

                event['description'] = non_descripts[event['event']]
            
            for old_name, new_name in NEW_TEAM_NAMES.items():
            
                event['description'] = event['description'].replace(old_name, new_name).upper()

            if game_id == 2012020018:

                bad_names = {'EDM #9': 'VAN #9', 'VAN #93': 'EDM #93', 'VAN #94': 'EDM #94'}

                for bad_name, good_name in bad_names.items():

                    event['description'] = event['description'].replace(bad_name, good_name)

            if game_id == 2018021133:

                event['description'] = event['description'].replace('WSH TAKEAWAY - #71 CIRELLI', 'TBL TAKEAWAY - #71 CIRELLI')

            if game_id == 2021020224:

                event['description'] = event['description'].replace(' - MTL #60 BELZILE VS BOS #92 NOSEK','MTL WON NEU. ZONE - MTL #60 BELZILE VS BOS #92 NOSEK')

            if game_id == 2018020989:

                event['time'] = event['time'].replace('-16:0-120:00', '5:000:00')

            if game_id == 2017020463:

                event['time'] = event['time'].replace('-16:0-120:00', '2:022:58')

            if game_id == 2016021127:

                event['description'] = event['description'].replace('BOS #55 ACCIARI ( MIN), DEF. ZONE', 'BOS #55 ACCIARI MISCONDUCT (10 MIN), DEF. ZONE')

            if game_id == 2015020904:

                event['time'] = event['time'].replace('-16:0-120:00', '5:000:00')

            if game_id == 2014021118:

                event['time'] = event['time'].replace('-16:0-120:00', '5:000:00')

            if game_id == 2013020083:

                event['time'] = event['time'].replace('-16:0-120:00', '5:000:00')

            if game_id == 2013020274:

                event['time'] = event['time'].replace('-16:0-120:00', '5:000:00')

            if game_id == 2013020644:

                event['time'] = event['time'].replace('-16:0-120:00', '5:000:00')

            if game_id == 2013020971:

                if event['event_idx'] == 1:

                    event['period'] = 1

                    event['time'] = '0:0020:00'

            if event['event'] == 'PEND' and event['time'] == '-16:0-120:00':

                goals = [x for x in events if x['period'] == event['period'] and x['event'] == 'GOAL']

                if len(goals) == 0:

                    if int(event['period']) == 4 and event['session'] == 'R':

                        event['time'] = event['time'].replace('-16:0-120:00', '5:000:00')

                    else:

                        event['time'] = event['time'].replace('-16:0-120:00', '20:000:00')

                elif len(goals) > 0:

                    goal = goals[-1]

                    event['time'] = event['time'].replace('-16:0-120:00', goal['time'])
                
            non_team_events = ['STOP', 'ANTHEM', 'PGSTR', 'PGEND', 'PSTR', 'PEND', 'EISTR', 'EIEND', 'GEND', 'SOC']
                    
            if event['event'] not in non_team_events:
                    
                try:

                    event['event_team'] = re.search(event_team_re, event['description']).group(1)

                    if event['event_team'] == 'LEA':

                        event['event_team'] = ''

                except AttributeError:

                    continue

            if event['event'] == 'FAC':
                
                event['event_team'] = re.search(fo_team_re, event['description']).group(1)

            if event['event'] == 'BLOCK' and 'BLOCKED BY' in event['description']:

                event['event_team'] = re.search(block_team_re, event['description']).group(1)

            event['period'] = int(event['period'])
                
            og_time = event['time']

            time_split = event['time'].split(':')

            event['period_time'] = time_split[0] + ':' + time_split[1][:2]

            event['period_seconds'] = (60 * int(event['period_time'].split(':')[0])) + int(event['period_time'].split(':')[1])

            event['game_seconds'] = (int(event['period']) - 1) * 1200 + event['period_seconds']

            if event['period'] == 5 and game_session == 'R':

                event['game_seconds'] = 3900 + event['period_seconds']

            event_list = ['GOAL', 'SHOT', 'TAKE', 'GIVE']
                    
            if event['event'] in event_list:

                event_players = [event['event_team'] + num for num in re.findall(numbers_re, event['description'])]

            else:

                event_players = re.findall(event_players_re, event['description'])

            if event['event'] == 'FAC' and event['event_team'] not in event_players[0]:

                event_players[0], event_players[1] = event_players[1], event_players[0]

            if event['event'] == 'BLOCK' and event['event_team'] not in event_players[0]:

                event_players[0], event_players[1] = event_players[1], event_players[0]

            for idx, event_player in enumerate(event_players):

                num = idx + 1
                
                event_player = event_player.replace(' #', '')

                try:
                
                    player_name = actives[event_player]['player_name']

                    eh_id = actives[event_player]['eh_id']

                    position = actives[event_player]['position']

                except KeyError:

                    player_name = scratches[event_player]['player_name']

                    eh_id = scratches[event_player]['eh_id']

                    position = scratches[event_player]['position']
                
                new_values = {f'player_{num}': player_name,
                              f'player_{num}_eh_id': eh_id,
                              f'player_{num}_position': position,
                             }
                
                event.update(new_values)
                
            try:

                event['zone'] = re.search(zone_re, event['description']).group(1).upper()

                if 'BLOCK' in event['event'] and event['zone'] == 'DEF':

                    event['zone'] = 'OFF'

            except AttributeError:

                continue

            penalty_events = ['PENL', 'DELPEN'] 

            if event['event'] in penalty_events:

                if ('TEAM' in event['description'] or 'HEAD COACH' in event['description']) and 'SERVED BY' in event['description']:

                    event['player_1'] = 'BENCH'

                    event['player_1_eh_id'] = 'BENCH'

                    event['player_1_position'] = ''

                    try:

                        served_by = re.search(served_re, event['description'])

                        served_name = served_by.group(1) + str(served_by.group(2))

                        event[f'player_2'] = actives[served_name]['player_name']

                        event[f'player_2_eh_id'] = actives[served_name]['eh_id']

                        event[f'player_2_position'] = actives[served_name]['position']

                    except AttributeError:

                        continue

                if 'SERVED BY' in event['description'] and 'DRAWN BY' in event['description']:

                    try:

                        drawn_by = re.search(drawn_re, event['description'])

                        drawn_name = drawn_by.group(1) + str(drawn_by.group(2))

                        event[f'player_2'] = actives[drawn_name]['player_name']

                        event[f'player_2_eh_id'] = actives[drawn_name]['eh_id']

                        event[f'player_2_position'] = actives[drawn_name]['position']

                        served_by = re.search(served_re, event['description'])

                        served_name = served_by.group(1) + str(served_by.group(2))

                        event[f'player_3'] = actives[served_name]['player_name']

                        event[f'player_3_eh_id'] = actives[served_name]['eh_id']

                        event[f'player_3_position'] = actives[served_name]['position']

                    except AttributeError:

                        continue

                elif 'SERVED BY' in event['description']:

                    try:

                        served_by = re.search(served_re, event['description'])

                        served_name = served_by.group(1) + str(served_by.group(2))

                        event[f'player_2'] = actives[served_name]['player_name']

                        event[f'player_2_eh_id'] = actives[served_name]['eh_id']

                        event[f'player_2_position'] = actives[served_name]['position']

                    except AttributeError:

                        continue

                elif 'DRAWN BY' in event['description']:

                    try:

                        drawn_by = re.search(drawn_re, event['description'])

                        drawn_name = drawn_by.group(1) + str(drawn_by.group(2))

                        event[f'player_2'] = actives[drawn_name]['player_name']

                        event[f'player_2_eh_id'] = actives[drawn_name]['eh_id']

                        event[f'player_2_position'] = actives[drawn_name]['position']

                    except AttributeError:

                        continue
  
                try:

                    event['penalty'] = re.search(penalty_re, event['description']).group(1).upper()
                    
                except AttributeError:
                    
                    continue
                
                if ('INTERFERENCE' in event['description'] and
                    'GOALKEEPER' in event['description']):
                    
                    event['penalty'] = 'GOALKEEPER INTERFERENCE'

                elif ('CROSS' in event['description'] and 
                    'CHECKING' in event['description']):

                    event['penalty'] = 'CROSS-CHECKING'

                elif ('DELAY' in event['description'] and
                    'GAME' in event['description'] and
                    'PUCK OVER' in event['description']):

                    event['penalty'] = 'DELAY OF GAME - PUCK OVER GLASS'

                elif ('DELAY' in event['description'] and
                    'GAME' in event['description'] and
                    'FO VIOL' in event['description']):

                    event['penalty'] = 'DELAY OF GAME - FACEOFF VIOLATION'

                elif ('DELAY' in event['description'] and
                    'GAME' in event['description'] and
                    'EQUIPMENT' in event['description']):

                    event['penalty'] = 'DELAY OF GAME - EQUIPMENT'

                elif ('DELAY' in event['description'] and
                    'GAME' in event['description'] and
                    'UNSUCC' in event['description']):

                    event['penalty'] = 'DELAY OF GAME - UNSUCCESSFUL CHALLENGE'

                elif ('DELAY' in event['description'] and
                    'GAME' in event['description'] and
                    'SMOTHERING' in event['description']):

                    event['penalty'] = 'DELAY OF GAME - SMOTHERING THE PUCK'

                elif ('ILLEGAL' in event['description'] and 
                    'CHECK' in event['description'] and 
                    'HEAD' in event['description']
                    ):

                    event['penalty'] = 'ILLEGAL CHECK TO HEAD'

                elif ('HIGH-STICKING' in event['description'] and 
                    '- DOUBLE' in event['description']):

                    event['penalty'] = 'HIGH-STICKING - DOUBLE MINOR'

                elif ('GAME MISCONDUCT' in event['description']):

                    event['penalty'] = 'GAME MISCONDUCT'

                elif ('MATCH PENALTY' in event['description']):

                    event['penalty'] = 'MATCH PENALTY'

                elif ('NET' in event['description'] and 'DISPLACED' in event['description']):

                    event['penalty'] = "DISPLACED NET"

                elif ('THROW' in event['description'] and 'OBJECT' in event['description'] and 'AT PUCK' in event['description']):

                    event['penalty'] = "THROWING OBJECT AT PUCK"

                elif ('INSTIGATOR' in event['description'] and 'FACE SHIELD' in event['description']):

                    event['penalty'] = "INSTIGATOR - FACE SHIELD"

                elif ('GOALIE LEAVE CREASE' in event['description']):

                    event['penalty'] = "LEAVING THE CREASE"

                elif ('REMOVING' in event['description'] and 'HELMET' in event['description']):

                    event['penalty'] = "REMOVING OPPONENT HELMET"

                elif ('BROKEN' in event['description'] and 'STICK' in event['description']):

                    event['penalty'] = "HOLDING BROKEN STICK"

                elif ('HOOKING' in event['description'] and 'BREAKAWAY' in event['description']):

                    event['penalty'] = 'HOOKING - BREAKAWAY'

                elif ('HOLDING' in event['description'] and 'BREAKAWAY' in event['description']):

                    event['penalty'] = 'HOLDING - BREAKAWAY'

                elif ('TRIPPING' in event['description'] and 'BREAKAWAY' in event['description']):

                    event['penalty'] = 'TRIPPING - BREAKAWAY'

                elif ('SLASH' in event['description'] and 'BREAKAWAY' in event['description']):

                    event['penalty'] = 'SLASHING - BREAKAWAY'

                elif ('TEAM TOO MANY' in event['description']):

                    event['penalty'] = 'TOO MANY MEN ON THE ICE'

                elif ('HOLDING' in event['description'] and 'STICK' in event['description']):

                    event['penalty'] = 'HOLDING THE STICK'

                elif ('THROWING' in event['description'] and 'STICK' in event['description']):

                    event['penalty'] = 'THROWING STICK'

                elif ('CLOSING' in event['description'] and 'HAND' in event['description']):

                    event['penalty'] = 'CLOSING HAND ON PUCK'

                elif ('ABUSE' in event['description'] and 'OFFICIALS' in event['description']):

                    event['penalty'] = 'ABUSE OF OFFICIALS'

                elif ('UNSPORTSMANLIKE CONDUCT' in event['description']):

                    event['penalty'] = 'UNSPORTSMANLIKE CONDUCT'

                elif ('PUCK' in event['description'] and 'THROWN' in event['description'] and 'FWD' in event['description']):

                    event['penalty'] = 'PUCK THROWN FORWARD - GOALKEEPER'

                elif ('DELAY' in event['description'] and 'GAME' in event['description']):

                    event['penalty'] = 'DELAY OF GAME'

                elif event['penalty'] == 'MISCONDUCT':

                    event['penalty'] = 'GAME MISCONDUCT'


                event['penalty_length'] = int(re.search(penalty_length_re, event['description']).group(1))

            shot_events = ['GOAL', 'SHOT', 'MISS', 'BLOCK']

            if event['event'] in shot_events:

                try:

                    event['shot_type'] = re.search(shot_re, event['description']).group(1).upper()

                except AttributeError:

                    event['shot_type'] = 'WRIST'

                    continue

            try:

                event['pbp_distance'] = int(re.search(distance_re, event['description']).group(1))

            except AttributeError:

                if event['event'] in ['GOAL', 'SHOT', 'MISS']:

                    event['pbp_distance'] = 0

                continue

        events = sorted(events, key = lambda k: (k['event_idx']))
                
        for event in events:

            if 'period_seconds' not in event.keys():
            
                if 'time' in event.keys():

                    event['period'] = int(event['period'])
                    
                    time_split = event['time'].split(':')
                    
                    event['period_time'] = time_split[0] + ':' + time_split[1][:2]

                    event['period_seconds'] = (60 * int(event['period_time'].split(':')[0])) + int(event['period_time'].split(':')[1])

            if 'game_seconds' not in event.keys():

                event['game_seconds'] = (int(event['period']) - 1) * 1200 + event['period_seconds']
                    
                if event['period'] == 5 and event['session'] == 'R':

                    event['game_seconds'] = 3900 + event['period_seconds']

            if 'version' not in event.keys():

                other_events = [x for x in events
                                if x != event
                                and x['event'] == event['event']
                                and x.get('game_seconds') == event['game_seconds']
                                and x['period'] == event['period']
                                and x.get('player_1_eh_id') is not None
                                and event.get('player_1_eh_id') is not None
                                and x['player_1_eh_id'] == event['player_1_eh_id']
                             ]

                version = 1

                event['version'] = version

                if len(other_events) > 0:

                    for idx, other_event in enumerate(other_events):

                        if 'version' not in other_event.keys():

                            version += 1

                            other_event['version'] = version

            
        games_dict.update({game_id: events})
                
        if game_id == game_ids[-1]:
            
            pbar.set_description(f'Finished scraping events data from the HTML endpoint')
            
            if nested == False:

                s.close()

        else:

            pbar.set_description(f'Finished scraping {game_id}')

        now = datetime.now()

        current_time = now.strftime("%H:%M:%S")

        postfix_str = f'{current_time}'

        pbar.set_postfix_str(postfix_str)

    if nested == False:

        events_data = [event for events in games_dict.values() for event in events]

        df = pd.DataFrame(events_data)

        columns = ['season', 'session', 'game_id', 'event_team', 'event_idx', 
                    'period', 'period_seconds', 'game_seconds',
                    'event', 'description', 'strength', 'zone', 'player_1',
                    'player_1_eh_id', 'player_1_position', 'player_2', 'player_2_eh_id',
                    'player_2_position', 'player_3',
                    'player_3_eh_id', 'player_3_position', 'pbp_distance', 'shot_type', 'penalty',
                    'penalty_length', 'version']

        column_order = [x for x in columns if x in df.columns]

        df = df[column_order]

        df = df.replace('', np.nan)

        return df

    else:

        return games_dict

############################################## PBP functions ##############################################

## []Refactored
## [x]Docstring
## []Comments
def prep_pbp(game_id, game_info, html_events, api_events, changes, rosters):
    '''

    --------- INFO ---------

    Preps the play-by-play data by combining HTML and API events, changes, and rosters. 
    Used only within the scrape_pbp function.

    Returns a list of event-dictionaries as values.

    --------- REQUIRED PARAMETER(S) ---------

    game_id | integer or list-like object
        A single 10-digit API game ID (e.g., 2022020002) or list-like object of 10-digit game IDs (e.g., generator or Pandas Series)

    html_events | JSON object: default = None
        When using in another scrape function, can pass the live endpoint response as a JSON object to prevent redundant hits

    api_events | requests Session object: default = None
        When using in another scrape function, can pass the requests session to improve speed

    changes | boolean: default = True
        If True, progress bar is disabled and returns a dictionary with game IDs as keys and lists of changes as dictionaries 
        If False, prints progress to the console and returns a dataframe

    rosters | boolean: default = True
        If True, progress bar is disabled and returns a dictionary with game IDs as keys and lists of changes as dictionaries 
        If False, prints progress to the console and returns a dataframe

    --------- RETURNS ---------

    Dictionary with game IDs as keys and lists of event-dictionaries as the values

    '''

    game_list = []

    game_info = game_info[game_id]
    html_events = html_events[game_id]
    api_events = api_events[game_id]
    changes = changes[game_id]
    rosters = rosters[game_id]
    
    for event in html_events:

        if event['event'] == 'EGPID':

            continue
        
        event_data = {}
        
        event_data.update(event)

        non_team_events = ['STOP', 'ANTHEM', 'PGSTR', 'PGEND', 'PSTR', 'PEND', 'EISTR', 'EIEND', 'GEND', 'SOC', 'EGT']

        if event['event'] in non_team_events:

            api_matches = [x for x in api_events if x['event'] == event['event']
                           and x['period'] == event['period']
                           and x['period_seconds'] == event['period_seconds']
                           and x['version'] == event['version']]


        elif event['event'] == 'CHL' and event.get('event_team', '') == '':

            api_matches = [x for x in api_events if x['event'] == event['event']
                           and x['period'] == event['period']
                           and x['period_seconds'] == event['period_seconds']
                           and x['version'] == event['version']]

        elif event['event'] == 'CHL' and event.get('event_team') is not None:

            api_matches = [x for x in api_events if x['event'] == event['event']
                            and x.get('event_team') is not None
                            and event.get('event_team') is not None
                            and x['event_team'] == event['event_team']
                           and x['period'] == event['period']
                           and x['period_seconds'] == event['period_seconds']
                           and x['version'] == event['version']]

        elif event['event'] == 'PENL': 

            api_matches = [x for x in api_events if x['event'] == event['event']
                            and x['event_team'] == event['event_team']
                            and x['player_1_eh_id'] == event['player_1_eh_id']
                            and x.get('player_2_eh_id') == event.get('player_2_eh_id') 
                            and x.get('player_3_eh_id') == event.get('player_3_eh_id') 
                           and x['period'] == event['period']
                           and x['period_seconds'] == event['period_seconds']]

        else:
        
            api_matches = [x for x in api_events if x['event'] == event['event']
                            and x.get('event_team') is not None
                            and event.get('event_team') is not None
                            and x['event_team'] == event['event_team']
                            and x.get('player_1_eh_id') is not None
                            and event.get('player_1_eh_id') is not None
                            and x['player_1_eh_id'] == event['player_1_eh_id']
                           and x['period'] == event['period']
                           and x['period_seconds'] == event['period_seconds']
                           and x['version'] == event['version']]

        if event['event'] == 'FAC' and len(api_matches) == 0:

            api_matches = [x for x in api_events if x['event'] == event['event']
                           and x['period'] == event['period']
                           and x['period_seconds'] == event['period_seconds']
                           and x['version'] == event['version']]
        
        if len(api_matches) == 0:
            
            game_list.append(event_data)
            
            continue
            
        elif len(api_matches) == 1:
            
            api_match = api_matches[0]
            
            new_values = {'event_idx_api': api_match['event_idx'],
                          'event_type': api_match['event_type'],
                          'api_description': api_match['description'],
                          'coords_x': api_match['coords_x'],
                          'coords_y': api_match['coords_y'],
                          'home_score': api_match['home_score'],
                          'away_score': api_match['away_score'],
                          'event_detail': api_match['event_detail'],
                          'event_dt': api_match['event_dt'],
                          'player_1_eh_id_api': api_match.get('player_1_eh_id', ''),
                          'player_1_api_id': api_match.get('player_1_api_id', ''),
                          'player_1_age': api_match.get('player_1_age', ''),
                          'player_1_hand': api_match.get('player_1_hand', ''),
                          'player_1_type': api_match.get('player_1_type', ''),
                          'player_2_eh_id_api': api_match.get('player_2_eh_id', ''),
                          'player_2_api_id': api_match.get('player_2_api_id', ''),
                          'player_2_age': api_match.get('player_2_age', ''),
                          'player_2_hand': api_match.get('player_2_hand', ''),
                          'player_2_type': api_match.get('player_2_type', ''),
                          'player_3_eh_id_api': api_match.get('player_3_eh_id', ''),
                          'player_3_api_id': api_match.get('player_3_api_id', ''),
                          'player_3_age': api_match.get('player_3_age', ''),
                          'player_3_hand': api_match.get('player_3_hand', ''),
                          'player_3_type': api_match.get('player_3_type', ''),
                          'time_elapsed': api_match['time_elapsed'],
                          'time_elapsed_seconds': api_match['time_elapsed_seconds'],
                          'version_api': api_match['version'],
                         }
            
            event_data.update(new_values)
            
            game_list.append(event_data)            
            
    game_list.extend(changes)
    
    for event in game_list:

        if event.get('player_2_type') == 'GOALIE' or event.get('player_2_type') == 'UNKNOWN':

            new_values = {'player_2': '',
                            'player_2_eh_id': '',
                            'player_2_eh_id_api': '',
                            'player_2_api_id': '',
                            'player_2_age': '',
                            'player_2_hand': '',
                            'player_2_type': ''}

            event.update(new_values)

        elif event.get('player_3_type') == 'GOALIE' or event.get('player_3_type') == 'UNKNOWN':

            new_values = {'player_3': '',
                            'player_3_eh_id': '',
                            'player_3_eh_id_api': '',
                            'player_3_api_id': '',
                            'player_3_age': '',
                            'player_3_hand': '',
                            'player_3_type': ''}

            event.update(new_values)

        new_values = {'game_date': game_info['game_date'],
                        'home_team': game_info['home_team'],
                        'home_team_name': game_info['home_team_name'],
                        'away_team': game_info['away_team'],
                        'away_team_name': game_info['away_team_name']
        }

        event.update(new_values)

        if 'event_type' not in event.keys():

            non_descripts = {'PGSTR': 'PRE-GAME START',
                                'PGEND': 'PRE-GAME END',
                                'ANTHEM': 'NATIONAL ANTHEM',
                                'EISTR': 'EARLY INTERMISSION START',
                                'EIEND': 'EARLY INTERMISSION END',
                                'DELPEN': 'DELAYED PENALTY' }

            event['event_type'] = non_descripts.get(event['event'], '')
                
        if 'version' not in event.keys():
            
            event['version'] = 1

        if event['period'] == 5 and event['session'] == 'R':

            event['sort_value'] = event['event_idx']

        else:
        
            sort_dict = {'PGSTR': 1,
                         'PGEND': 2,
                         'ANTHEM': 3,
                         'EGT': 3,
                         'CHL': 3,
                         'DELPEN': 3,
                         'BLOCK': 3,
                         'GIVE': 3,
                         'HIT': 3,
                         'MISS': 3,
                         'SHOT': 3,
                         'TAKE': 3,
                         'PENL': 4,
                         'GOAL': 5, 
                         'STOP': 6,
                         'PSTR': 7,
                         'CHANGE': 8,
                         'EISTR': 9,
                         'EIEND': 10,
                         'FAC': 12,
                         'PEND': 13,
                         'SOC': 14,
                         'GEND': 15,
                         'GOFF': 16
                        }

            event['sort_value'] = sort_dict[event['event']]
            
    game_list = sorted(game_list, key = lambda k: (k['period'], k['period_seconds'], k['sort_value'])) #, k['version']
    
    for event in game_list:

        event['home_forwards_id'] = []
        event['home_forwards'] = []
        event['home_forwards_positions'] = []
        event['home_forwards_ages'] = []
        event['home_forwards_hands'] = []
        event['home_defense_id'] = []
        event['home_defense'] = []
        event['home_defense_positions'] = []
        event['home_defense_ages'] = []
        event['home_defense_hands'] = []
        event['home_goalie_id'] = []
        event['home_goalie'] = []
        event['home_goalie_age'] = []
        event['home_goalie_catches'] = []

        event['away_forwards_id'] = []
        event['away_forwards'] = []
        event['away_forwards_positions'] = []
        event['away_forwards_ages'] = []
        event['away_forwards_hands'] = []
        event['away_defense_id'] = []
        event['away_defense'] = []
        event['away_defense_positions'] = []
        event['away_defense_ages'] = []
        event['away_defense_hands'] = []
        event['away_goalie_id'] = []
        event['away_goalie'] = []
        event['away_goalie_age'] = []
        event['away_goalie_catches'] = []
    
    roster = [x for x in rosters if x['status'] == 'ACTIVE']
    
    teams = np.unique([x['team'] for x in roster]).tolist()
    
    roster = sorted(roster, key = lambda k: (k['team_venue'], k['jersey']))
        
    for player in roster:

        counter = 0

        for event in game_list:

            if (event.get('event_team', 'NaN') in player['team_jersey'] 
                and event['event'] == 'CHANGE'
                and event.get('change_on') is not None):
                
                players_on = [x for x in event['change_on_jersey'] if x == player['team_jersey']]
                
                if len(players_on) > 0:

                    counter += 1

            if (event.get('event_team', 'NaN') in player['team_jersey']
                and event['event'] == 'CHANGE'
                and event.get('change_off') is not None):
                
                players_off = [x for x in event['change_off_jersey'] if x == player['team_jersey']]
                
                if len(players_off) > 0:

                    counter -= 1
                    
            if counter > 0:

                forwards = ['L', 'C', 'R']
                
                if player['team_venue'] == 'HOME':

                    if player['position'] in forwards:

                        event['home_forwards_id'].append(player['eh_id'])

                        event['home_forwards'].append(player['player_name'])

                        event['home_forwards_positions'].append(player['position'])

                        event['home_forwards_ages'].append(round(player['age'], 2))

                        event['home_forwards_hands'].append(player['shoots'])

                    elif player['position'] == 'D':

                        event['home_defense_id'].append(player['eh_id'])

                        event['home_defense'].append(player['player_name'])

                        event['home_defense_positions'].append(player['position'])

                        event['home_defense_ages'].append(round(player['age'], 2))

                        event['home_defense_hands'].append(player['shoots'])

                    elif player['position'] == 'G':

                        event['home_goalie_id'].append(player['eh_id'])

                        event['home_goalie'].append(player['player_name'])

                        event['home_goalie_age'].append(round(player['age'], 2))

                        event['home_goalie_catches'].append(player['catches'])
                    
                else:

                    if player['position'] in forwards:

                        event['away_forwards_id'].append(player['eh_id'])

                        event['away_forwards'].append(player['player_name'])

                        event['away_forwards_positions'].append(player['position'])

                        event['away_forwards_ages'].append(round(player['age'], 2))

                        event['away_forwards_hands'].append(player['shoots'])

                    elif player['position'] == 'D':

                        event['away_defense_id'].append(player['eh_id'])

                        event['away_defense'].append(player['player_name'])

                        event['away_defense_positions'].append(player['position'])

                        #display(player)

                        event['away_defense_ages'].append(round(player['age'], 2))

                        event['away_defense_hands'].append(player['shoots'])

                    elif player['position'] == 'G':

                        event['away_goalie_id'].append(player['eh_id'])

                        event['away_goalie'].append(player['player_name'])

                        event['away_goalie_age'].append(round(player['age'], 2))

                        event['away_goalie_catches'].append(player['catches'])
                
    for idx, event in enumerate(game_list):

        new_values = {'event_idx': idx + 1,
                        #'event_length': event['game_seconds'] - game_list[idx - 1]['game_seconds'],
                        'home_on_id': event['home_forwards_id'] + event['home_defense_id'],
                        'home_on': event['home_forwards'] + event['home_defense'],
                        'home_on_positions': event['home_forwards_positions'] + event['home_defense_positions'],
                        'home_on_ages': event['home_forwards_ages'] + event['home_defense_ages'],
                        'home_on_hands': event['home_forwards_hands'] + event['home_defense_hands'],
                        'away_on_id': event['away_forwards_id'] + event['away_defense_id'],
                        'away_on': event['away_forwards'] + event['away_defense'],
                        'away_on_positions': event['away_forwards_positions'] + event['away_defense_positions'],
                        'away_on_ages': event['away_forwards_ages'] + event['away_defense_ages'],
                        'away_on_hands': event['away_forwards_hands'] + event['away_defense_hands'],
                        }

        event.update(new_values)

        if event.get('event_team') == event['home_team']:

            event['is_home'] = 1

        else:

            event['is_home'] = 0

        if event.get('event_team') == event['away_team']:

            event['is_away'] = 1

        else:

            event['is_away'] = 0

        if (event.get('coords_x') is not None and
            event.get('coords_y') is not None):
            
            

            ## Fixing event angle and distance for errors

            is_fenwick = event['event'] in ['GOAL', 'SHOT', 'MISS']
            is_long_distance = event.get('pbp_distance', 0) > 89
            x_is_neg = event['coords_x'] < 0 
            x_is_pos = event['coords_x'] > 0 
            bad_shots = event.get('shot_type', 'WRIST') not in ['TIP-IN', 'WRAP-AROUND', 'WRAP', 'DEFLECTED', 'BAT']
            zone_cond = (event.get('pbp_distance', 0) > 89 and event.get('zone') == 'OFF')

            x_is_neg_conds = (is_fenwick & is_long_distance & x_is_neg & bad_shots & ~zone_cond)
            x_is_pos_conds = (is_fenwick & is_long_distance & x_is_pos & bad_shots & ~zone_cond)
            
            if x_is_neg_conds == True:

                event['event_distance'] = ((abs(event['coords_x']) + 89) ** 2 + event['coords_y'] ** 2) ** (1/2)

                try:

                    event['event_angle'] = np.degrees(abs(np.arctan(event['coords_y']/(abs(event['coords_x'] + 89)))))

                except ZeroDivisionError:

                    event['event_angle'] = np.degrees(abs(np.arctan(np.nan)))

            elif x_is_pos_conds == True:

                event['event_distance'] = ((event['coords_x'] + 89) ** 2 + event['coords_y'] ** 2) ** (1/2)

                try:

                    event['event_angle'] = np.degrees(abs(np.arctan(event['coords_y']/(event['coords_x'] + 89))))

                except ZeroDivisionError:

                    event['event_angle'] = np.degrees(abs(np.arctan(np.nan)))

            else:

                event['event_distance'] = ((89 - abs(event['coords_x']))**2 + event['coords_y']**2) ** (1/2)

                try:
                    
                    event['event_angle'] = np.degrees(abs(np.arctan(event['coords_y'] / (89 - abs(event['coords_x'])))))

                except ZeroDivisionError:

                    event['event_angle'] = np.degrees(abs(np.arctan(np.nan)))

        if event['event'] in ['GOAL', 'SHOT', 'MISS'] and event.get('zone') == 'DEF' and event.get('event_distance', 0) <= 64:

            event['zone'] = 'OFF'

        event['home_skaters'] = len(event['home_on_id'])

        event['away_skaters'] = len(event['away_on_id'])

        if event['home_goalie'] == []:

            home_on = 'E'

        else:

            home_on = event['home_skaters']

        if event['away_goalie'] == []:

            away_on = 'E'

        else:

            away_on = event['away_skaters']

        event['strength_state'] = f"{home_on}v{away_on}"

        if 'PENALTY SHOT' in event['description']:

            event['strength_state'] = f"1v0"

        if event.get('event_team') == event['home_team']:

            new_values = {'strength_state': f"{home_on}v{away_on}",
                            'event_team_skaters': event['home_skaters'],
                            'event_team_on_id': event['home_on_id'],
                            'event_team_on': event['home_on'],
                            'event_team_on_positions': event['home_on_positions'],
                            'event_team_on_ages': event['home_on_ages'],
                            'event_team_on_hands': event['home_on_hands'],
                            'event_team_forwards_id': event['home_forwards_id'],
                            'event_team_forwards': event['home_forwards'],
                            'event_team_forwards_ages': event['home_forwards_ages'],
                            'event_team_forwards_hands': event['home_forwards_hands'],
                            'event_team_defense_id': event['home_defense_id'],
                            'event_team_defense': event['home_defense'],
                            'event_team_defense_ages': event['home_defense_ages'],
                            'event_team_defense_hands': event['home_defense_hands'],
                            'event_team_goalie_id': event['home_goalie_id'],
                            'event_team_goalie': event['home_goalie'],
                            'event_team_goalie_age': event['home_goalie_age'],
                            'event_team_goalie_catches': event['home_goalie_catches'],
                            'opp_team_skaters': event['away_skaters'],
                            'opp_team_on_id': event['away_on_id'],
                            'opp_team_on': event['away_on'],
                            'opp_team_on_positions': event['away_on_positions'],
                            'opp_team_on_ages': event['away_on_ages'],
                            'opp_team_on_hands': event['away_on_hands'],
                            'opp_team_forwards_id': event['away_forwards_id'],
                            'opp_team_forwards': event['away_forwards'],
                            'opp_team_forwards_ages': event['away_forwards_ages'],
                            'opp_team_forwards_hands': event['away_forwards_hands'],
                            'opp_team_defense_id': event['away_defense_id'],
                            'opp_team_defense': event['away_defense'],
                            'opp_team_defense_ages': event['away_defense_ages'],
                            'opp_team_defense_hands': event['away_defense_hands'],
                            'opp_team_goalie_id': event['away_goalie_id'],
                            'opp_team_goalie': event['away_goalie'],
                            'opp_team_goalie_age': event['away_goalie_age'],
                            'opp_team_goalie_catches': event['away_goalie_catches'],
            }

            event.update(new_values)

        elif event.get('event_team') == event['away_team']:

            new_values = {'strength_state': f"{away_on}v{home_on}",
                            'event_team_skaters': event['away_skaters'],
                            'event_team_on_id': event['away_on_id'],
                            'event_team_on': event['away_on'],
                            'event_team_on_positions': event['away_on_positions'],
                            'event_team_on_ages': event['away_on_ages'],
                            'event_team_on_hands': event['away_on_hands'],
                            'event_team_forwards_id': event['away_forwards_id'],
                            'event_team_forwards': event['away_forwards'],
                            'event_team_forwards_ages': event['away_forwards_ages'],
                            'event_team_forwards_hands': event['away_forwards_hands'],
                            'event_team_defense_id': event['away_defense_id'],
                            'event_team_defense': event['away_defense'],
                            'event_team_defense_ages': event['away_defense_ages'],
                            'event_team_defense_hands': event['away_defense_hands'],
                            'event_team_goalie_id': event['away_goalie_id'],
                            'event_team_goalie': event['away_goalie'],
                            'event_team_goalie_age': event['away_goalie_age'],
                            'event_team_goalie_catches': event['away_goalie_catches'],
                            'opp_team_skaters': event['home_skaters'],
                            'opp_team_on_id': event['home_on_id'],
                            'opp_team_on': event['home_on'],
                            'opp_team_on_positions': event['home_on_positions'],
                            'opp_team_on_ages': event['home_on_ages'],
                            'opp_team_on_hands': event['home_on_hands'],
                            'opp_team_forwards_id': event['home_forwards_id'],
                            'opp_team_forwards': event['home_forwards'],
                            'opp_team_forwards_ages': event['home_forwards_ages'],
                            'opp_team_forwards_hands': event['home_forwards_hands'],
                            'opp_team_defense_id': event['home_defense_id'],
                            'opp_team_defense': event['home_defense'],
                            'opp_team_defense_ages': event['home_defense_ages'],
                            'opp_team_defense_hands': event['home_defense_hands'],
                            'opp_team_goalie_id': event['home_goalie_id'],
                            'opp_team_goalie': event['home_goalie'],
                            'opp_team_goalie_age': event['away_goalie_age'],
                            'opp_team_goalie_catches': event['home_goalie_catches'],
            }

            event.update(new_values)

        if ((event['home_skaters'] > 5 and event['home_goalie'] != [])
            or (event['away_skaters'] > 5 and event['away_goalie'] != [])):

            event['strength_state'] = 'ILLEGAL'

        if event['period'] == 5 and event['session'] == 'R':

            event['strength_state'] = '1v0'

        if event['event'] == 'CHANGE':

            faceoffs = [x for x in game_list if (x['event'] == 'FAC' and
                                                    x['game_seconds'] == event['game_seconds'] and
                                                    x['period'] == event['period'])]

            if len(faceoffs) > 0:

                event['coords_x'] = faceoffs[0]['coords_x']

                event['coords_y'] = faceoffs[0]['coords_y']

                if event['event_team'] == faceoffs[0]['event_team']:

                    event['zone'] = faceoffs[0]['zone']

                else:

                    zones = {'OFF': 'DEF', 'DEF': 'OFF', 'NEU': 'NEU'}

                    event['zone'] = zones[faceoffs[0]['zone']]

            else:

                event['zone'] = 'OTF'

    return {game_id: game_list}

## []Refactored
## []Docstring
## []Comments
def scrape_pbp(game_ids, nested = False, disable_print = False):
    
    '''

    --------- INFO ---------

    Scrapes play-by-play data from several HTML and API endpoints for a given game ID or list-like object of game IDs.

    By default returns a Pandas DataFrame.
    If nested is True, returns a dictionary with game IDs as keys and lists of event-dictionaries as values

    Scrapes approximately 1 game every 2-3 seconds. 

    --------- REQUIRED PARAMETER(S) ---------

    game_ids | integer or list-like object
        A single 10-digit API game ID (e.g., 2022020001) or list-like object of 10-digit game IDs (e.g., generator or Pandas Series)

    --------- OPTIONAL PARAMETER(S) ---------

    nested | boolean: default = False
        If True, returns a dictionary with game IDs as keys and lists of events as dictionaries 
        If False, returns a Pandas DataFrame

    disable_print | boolean: default = False
        When using in another scrape function, can pass the requests session to improve speed

    --------- RETURNS ---------

    Default: Pandas DataFrame with each event as a row and the columns listed below. 

    If nested = True, then returns a dictionary with game IDs as keys and lists of event-dictionaries as the values

    The play-by-play DataFrame returned contains the following fields:

        season: integer
            8-digit season code, e.g., 20192020

        session: object
            Regular season or playoffs, e.g., R

        game_id: integer
            10-digit game identifier, e.g., 2019020684

        game_date: object
            Date of game in Eastern time-zone, e.g., 2020-01-09

        period: integer
            Game period, e.g., 3

        period_seconds: integer
            Period time elapsed in seconds, e.g., 1178

        game_seconds: integer
            Game time elapsed in seconds, e.g., 3578

        strength_state: object
            Game time elapsed in seconds, e.g., 5vE

        event_idx: object
            Unique index number of event, in chronological order, e.g., 665

        event_team: object
            3-letter abbreviation of the team for the event, e.g., NSH 

        event: object
            Name of the event, e.g., GOAL

        event_type: object
            Type of the event, e.g., GOAL

        description: object
            Description of the event, e.g., NSH #35 RINNE(1), WRIST, DEF. ZONE, 185 FT.

        zone: object
            Zone location of event, e.g., DEF

        coords_x: float
            X coordinates of event, e.g., -96

        coords_y: float
            Y coordinates of event, e.g., 11

        player_1: object
            Name of the player, e.g.,  PEKKA RINNE

        player_1_eh_id: object
            Identifier that can be used to match with Evolving Hockey data, e.g., PEKKA.RINNE

        player_1_api_id: integer
            Unique ID for the player, e.g., 8471469

        player_2: object
            Name of the player, e.g.,  PEKKA RINNE

        player_2_eh_id: object
            Identifier that can be used to match with Evolving Hockey data, e.g., PEKKA.RINNE

        player_2_api_id: integer
            Unique ID for the player, e.g., 8471469

        player_3: object
            Name of the player, e.g.,  PEKKA RINNE

        player_3_eh_id: object
            Identifier that can be used to match with Evolving Hockey data, e.g., PEKKA.RINNE

        player_3_api_id: integer
            Unique ID for the player, e.g., 8471469
        
        shot_type: object
            Type of shot, e.g., WRIST

        pbp_distance: float
            Shot distance from net in feet, e.g., 185
        
        event_detail: object
            Additional information available for the event from the API, e.g., WRIST SHOT
        
        penalty: object
            Name of penalty

        penalty_length: object
            Length of penalty in minutes

        penalty_severity: object
            Whether penalty is a minor or major

        event_dt: object
            Datetime in eastern time zone that event occurred, e.g., Timestamp('2020-01-09 23:01:47-0500', tz='US/Eastern')

        time_elapsed: object
            Time delta object for amount of time elapsed since game start, e.g., 0 days 02:21:37

        time_elapsed_seconds: float
            Time elapsed since game start in seconds, e.g., 8497.0

        home_score: integer
            Number of goals scored by home team, e.g., 2

        away_score: integer
            Number of goals scored by away team, e.g., 5

        home: integer
            Whether event team is home, e.g., 0

        away: integer
            Whether event team is away, e.g., 1

        change_on_count: integer
            Number of players entering the ice, e.g., 6

        change_off_count: integer
            Number of players exiting the ice, e.g., 0

        change_on_jersey: list
            Abbreviations of players on, in jersey order, e.g., (NSH9, NSH45, NSH59, NSH64, NSH74, NSH95)
            If returning a DataFrame, will be a string, e.g., NSH9, NSH45, NSH59, NSH64, NSH74, NSH95

        change_on: list
            Names of players on, in jersey order, e.g., (FILIP FORSBERG, ALEX CARRIER, ROMAN JOSI, MIKAEL GRANLUND, JUUSE SAROS, MATT DUCHENE)
            If returning a DataFrame, will be a string, e.g., FILIP FORSBERG, ALEX CARRIER, ROMAN JOSI, MIKAEL GRANLUND, JUUSE SAROS, MATT DUCHENE

        change_on_id: list
            Evolving Hockey IDs of players on, in jersey order, e.g., (FILIP.FORSBERG, ALEX.CARRIER, ROMAN.JOSI, MIKAEL.GRANLUND, JUUSE.SAROS, MATT.DUCHENE)
            If returning a DataFrame, will be a string, e.g., FILIP.FORSBERG, ALEX.CARRIER, ROMAN.JOSI, MIKAEL.GRANLUND, JUUSE.SAROS, MATT.DUCHENE 

        change_on_positions: list
            Positions of players on, in jersey order, e.g., (L, D, D, C, G, C)
            If returning a DataFrame, will be a string, e.g., L, D, D, C, G, C

        change_off_jersey: list
            Abbreviations of players off, in jersey order
            If returning a DataFrame, will be a string

        change_off: list
            Names of players off, in jersey order
            If returning a DataFrame, will be a string

        change_off_id: list
            Evolving Hockey IDs of players off, in jersey order
            If returning a DataFrame, will be a string

        change_off_positions: list
            Positions of players off, in jersey order
            If returning a DataFrame, will be a string

        change_on_forwards_count: integer
            Number of forwards entering the ice, e.g., 6

        change_off_forwards_count: integer
            Number of forwards exiting the ice, e.g., 0

        change_on_forwards_jersey: list
            Abbreviations of forwards on, in jersey order, e.g., (NSH9, NSH64, NSH95)
            If returning a DataFrame, will be a string, e.g., NSH9, NSH64, NSH95

        change_on_forwards: list
            Names of forwards on, in jersey order, e.g., (FILIP FORSBERG, MIKAEL GRANLUND, MATT DUCHENE)
            If returning a DataFrame, will be a string, e.g., FILIP FORSBERG, MIKAEL GRANLUND, MATT DUCHENE

        change_on_forwards_id: list
            Evolving Hockey IDs of forwards on, in jersey order, e.g., (FILIP.FORSBERG, MIKAEL.GRANLUND, MATT.DUCHENE)
            If returning a DataFrame, will be a string, e.g., FILIP.FORSBERG, MIKAEL.GRANLUND, MATT.DUCHENE

        change_off_forwards_jersey: list
            Abbreviations of forwards off, in jersey order
            If returning a DataFrame, will be a string

        change_off_forwards: list
            Names of forwards off, in jersey order
            If returning a DataFrame, will be a string

        change_off_forwards_id: list
            Evolving Hockey IDs of forwards off, in jersey order
            If returning a DataFrame, will be a string

        change_on_defense_count: list
            Number of defense entering the ice, e.g., 6

        change_off_defense_count: list
            Number of defense exiting the ice, e.g., 0

        change_on_defense_jersey: list
            Abbreviations of defense on, in jersey order, e.g., (NSH45, NSH59)
            If returning a DataFrame, will be a string, e.g., NSH45, NSH59

        change_on_defense: list
            Names of defense on, in jersey order, e.g., (ALEX CARRIER, ROMAN JOSI)
            If returning a DataFrame, will be a string, e.g., ALEX CARRIER, ROMAN JOSI

        change_on_defense_id: list
            Evolving Hockey IDs of defense on, in jersey order, e.g., (ALEX.CARRIER, ROMAN.JOSI)
            If returning a DataFrame, will be a string, e.g., ALEX.CARRIER, ROMAN.JOSI

        change_off_defense_jersey: list
            Abbreviations of defense off, in jersey order
            If returning a DataFrame, will be a string

        change_off_defense: list
            Names of defense off, in jersey order
            If returning a DataFrame, will be a string

        change_off_defense_id: list
            Evolving Hockey IDs of defense off, in jersey order
            If returning a DataFrame, will be a string

        change_on_goalie_count: integer
            Number of goalies entering the ice, e.g., 6

        change_off_goalie_count: integer
            Number of goalies exiting the ice, e.g., 0

        change_on_goalie_jersey: list
            Abbreviations of goalies on, in jersey order, e.g., (NSH74)
            If returning a DataFrame, will be a string, e.g., NSH74

        change_on_goalie: list
            Names of goalies on, in jersey order, e.g., (JUUSE SAROS)
            If returning a DataFrame, will be a string, e.g., JUUSE SAROS

        change_on_goalie_id: list
            Evolving Hockey IDs of goalies on, in jersey order, e.g., (JUUSE.SAROS)            
            If returning a DataFrame, will be a string, e.g., JUUSE.SAROS

        change_off_goalie_jersey: list
            Abbreviations of goalies off, in jersey order
            If returning a DataFrame, will be a string

        change_off_goalie: list
            Names of goalies off, in jersey order
            If returning a DataFrame, will be a string

        change_off_goalie_id: list
            Evolving Hockey IDs of players off, in jersey order
            If returning a DataFrame, will be a string

    '''


    
    ## Convert game IDs to list if given a single game ID
    game_ids = convert_to_list(obj = game_ids, object_type = 'game ID')
    
    ## Important lists
    games_dict = {}
    
    s = s_session()
        
    pbar = tqdm(game_ids, disable = disable_print)
    
    for game_id in pbar:

        season, game_session = game_id_info(game_id)

        live_response = scrape_live_endpoint(game_id, session = s)

        game_info = scrape_game_info(game_id, live_response = live_response, session = s, nested = True)

        api_events = scrape_api_events(game_id, live_response = live_response, session = s, nested = True)

        api_rosters = scrape_api_rosters(game_id, live_response = live_response, session = s, nested = True)

        html_rosters = scrape_html_rosters(game_id, session = s, nested = True)

        rosters = scrape_rosters(game_id, html_rosters = html_rosters, api_rosters = api_rosters, session = s, nested = True)

        changes = scrape_changes(game_id, roster_data = rosters, session = s, nested = True)

        html_events = scrape_html_events(game_id, roster_data = rosters, session = s, nested = True)

        game_dict = prep_pbp(game_id, game_info, html_events, api_events, changes, rosters)

        games_dict.update(game_dict)

        if game_id == game_ids[-1]:
            
            pbar.set_description(f'Finished scraping play-by-play data')
            
        else:
        
            pbar.set_description(f'Finished scraping {game_id}')
        
        now = datetime.now()

        current_time = now.strftime("%H:%M:%S")

        postfix_str = f'{current_time}'
        
        pbar.set_postfix_str(postfix_str)

    if nested == False:

        events_data = [event for events in games_dict.values() for event in events]

        list_fields = ['home_on_id',
                         'home_on',
                         'home_on_positions',
                         'home_on_ages',
                         'home_on_hands',
                         'home_goalie_id',
                         'home_goalie',
                         'home_goalie_age',
                         'home_goalie_catches',
                         'away_on_id',
                         'away_on',
                         'away_on_positions',
                         'away_on_ages',
                         'away_on_hands',
                         'away_goalie_id',
                         'away_goalie',
                         'away_goalie_age',
                         'away_goalie_catches',
                         'event_team_on_id',
                         'event_team_on',
                         'event_team_on_positions',
                         'event_team_on_ages',
                         'event_team_on_hands',
                         'event_team_forwards_id',
                         'event_team_forwards',
                         'event_team_forwards_ages',
                         'event_team_forwards_hands',
                         'event_team_defense_id',
                         'event_team_defense',
                         'event_team_defense_ages',
                         'event_team_defense_hands',
                         'event_team_goalie_id',
                         'event_team_goalie',
                         'event_team_goalie_age',
                         'event_team_goalie_catches',
                         'opp_team_on_id',
                         'opp_team_on',
                         'opp_team_on_positions',
                         'opp_team_on_ages',
                         'opp_team_on_hands',
                         'opp_team_forwards_id',
                         'opp_team_forwards',
                         'opp_team_forwards_ages',
                         'opp_team_forwards_hands',
                         'opp_team_defense_id',
                         'opp_team_defense',
                         'opp_team_defense_ages',
                         'opp_team_defense_hands',
                         'opp_team_goalie_id',
                         'opp_team_goalie',
                         'opp_team_goalie_age',
                         'opp_team_goalie_catches',
                         'change_on',
                         'change_on_id',
                         'change_on_positions',
                         'change_off',
                         'change_off_id', 
                         'change_off_positions',
                         'change_on_forwards',
                         'change_on_forwards_id',
                         'change_off_forwards',
                         'change_off_forwards_id',
                         'change_on_defense',
                         'change_on_defense_id',
                         'change_off_defense',
                         'change_off_defense_id',
                         'change_on_goalie',
                         'change_on_goalie_id',
                         'change_off_goalie',
                         'change_off_goalie_id'
                         ]

        for event in events_data:

            for list_field in [x for x in list_fields if x in event.keys()]:

                if 'age' in list_field or 'ages' in list_field:

                    event[list_field] = ', '.join([str(x) for x in event[list_field]])

                else:

                    event[list_field] = ', '.join(event[list_field])

        df = pd.DataFrame(events_data)

        columns = ['season', 'session', 'game_id', 'game_date',
                   'period', 'period_seconds', 'game_seconds',
                   'strength_state', 'score_state', 'event_idx', 'event_team',
                   'event', 'event_type', 'description', 'zone',
                   'coords_x', 'coords_y', 'event_length', 'player_1', 'player_1_eh_id',
                   'player_1_api_id', 'player_1_age', 'player_1_hand', 'player_1_type', 'player_1_eh_id_api',
                   'player_2', 'player_2_eh_id',  'player_2_api_id', 'player_2_age', 'player_2_hand', 'player_2_type',
                   'player_2_eh_id_api', 
                   'player_3', 'player_3_eh_id', 'player_3_api_id', 'player_3_age', 'player_3_hand', 'player_3_type',
                   'player_3_eh_id_api', 
                   'shot_type', 'event_distance', 'event_angle', 'pbp_distance', 'event_detail', 
                   'penalty', 'penalty_length', 'penalty_severity', 'event_dt',
                   'time_elapsed', 'time_elapsed_seconds', 'home_score',
                   'away_score', 'is_home', 'is_away', 'home_team', 'home_team_name',
                   'away_team', 'away_team_name', 'home_skaters', 'away_skaters',
                   'event_team_skaters', 'event_team_on_id', 'event_team_on', 'event_team_on_positions',
                   'event_team_on_ages', 'event_team_on_hands',
                   'event_team_goalie_id', 'event_team_goalie', 'event_team_goalie_age', 'event_team_goalie_catches', 'event_team_forwards_id',
                   'event_team_forwards', 'event_team_forwards_ages', 'event_team_forwards_hands', 'event_team_defense_id', 'event_team_defense',
                   'event_team_defense_ages', 'event_team_defense_hands', 
                   'opp_team_skaters', 'opp_team_on_id', 'opp_team_on', 'opp_team_on_positions', 'opp_team_on_ages', 'opp_team_on_hands', 'opp_team_goalie_id',
                   'opp_team_goalie', 'opp_team_goalie_age', 'opp_team_goalie_catches', 'opp_team_forwards_id', 'opp_team_forwards',
                   'opp_team_forwards_ages', 'opp_team_forwards_hands', 'opp_team_defense_id',
                   'opp_team_defense', 'opp_team_defense_ages', 'opp_team_defense_hands', 'home_on_id', 'home_on', 'home_on_positions',
                   'home_on_ages', 'home_on_hands', 'home_goalie_id',
                   'home_goalie', 'home_goalie_age', 'home_goalie_catches',
                   'away_on_id', 'away_on', 'away_on_positions', 'away_on_ages', 'away_on_hands', 'away_goalie_id', 'away_goalie',
                   'away_goalie_age', 'away_goalie_catches', 'change_on_count', 'change_off_count', 'change_on', 'change_on_id', 'change_on_positions',
                   'change_off', 'change_off_id', 'change_off_positions', 'change_on_forwards_count',
                   'change_off_forwards_count', 'change_on_forwards', 'change_on_forwards_id', 'change_off_forwards',
                   'change_off_forwards_id', 'change_on_defense_count', 'change_off_defense_count',
                   'change_on_defense', 'change_on_defense_id',  'change_off_defense', 'change_off_defense_id',
                   'change_on_goalie_count', 'change_off_goalie_count',
                   'change_on_goalie', 'change_on_goalie_id','change_off_goalie', 'change_off_goalie_id', 'version'
        ]

        columns = [x for x in columns if x in df.columns]

        df = df[columns]

        df = df.replace('', np.nan).replace(' ', np.nan)

        return df

    else:

        return games_dict


## End









