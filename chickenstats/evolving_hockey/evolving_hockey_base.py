########################## Program to clean and upload evolving hockey data to SQL database ##########################

########################## Import dependencies ##########################

import pandas as pd
import numpy as np
import time
from pathlib import Path
from sqlalchemy import create_engine
import datetime
import psycopg2
from tqdm.auto import tqdm
from sqlalchemy import create_engine

########################## Functions ##########################

## Function to munge play-by-play data
def munge_pbp(pbp):
    
    '''
    Function to munge play-by-play data from Evolving-Hockey
    Returns a dataframe
    
    Arguments:
    
        pbp = Dataframe from the Evolving-Hockey play-by-play query
        
    '''
    
    df = pbp.copy()
    
    ## Common column names for ease of typing later
    
    EVENT_TEAM = df.event_team
    HOME_TEAM = df.home_team
    AWAY_TEAM = df.away_team
    EVENT_TYPE = df.event_type
    
    ## Adding opp_team
    
    conditions = [EVENT_TEAM == HOME_TEAM, EVENT_TEAM == AWAY_TEAM]
    values = [AWAY_TEAM, HOME_TEAM]
    
    df['opp_team'] = np.select(conditions, values, np.nan)
    
    ## Adding opp_goalie and own goalie
    
    values = [df.away_goalie, df.home_goalie]
    df['opp_goalie'] = np.select(conditions, values, np.nan) # Uses same conditions as opp_team
    df.opp_goalie = df.opp_goalie.fillna('EMPTY NET')
    
    values.reverse()
    df['own_goalie'] = np.select(conditions, values, np.nan) # Uses same conditions as opp_team
    df.own_goalie = df.own_goalie.fillna('EMPTY NET')
    
    ## Adding event_on and opp_on
    
    for num in range(1, 8):
        
        home = df[f'home_on_{num}']
        away = df[f'away_on_{num}']
        
        conditions = [EVENT_TEAM == HOME_TEAM, EVENT_TEAM == AWAY_TEAM]
        values = [home, away]
        
        df[f'event_on_{num}'] = np.select(conditions, values, np.nan)
        
        values.reverse()
        
        df[f'opp_on_{num}'] = np.select(conditions, values, np.nan)
    
    ## Adding zone_start
    
    conds_1 = np.logical_and(np.logical_and(EVENT_TYPE == 'CHANGE',
                                            EVENT_TYPE.shift(-1) == 'FAC'),
                             np.logical_and(df.game_seconds == df.game_seconds.shift(-1),
                                            df.game_period == df.game_period.shift(-1)))
    
    conds_2 = np.logical_and(np.logical_and(EVENT_TYPE == 'CHANGE',
                                            EVENT_TYPE.shift(-2) == 'FAC'),
                             np.logical_and(df.game_seconds == df.game_seconds.shift(-2),
                                            df.game_period == df.game_period.shift(-2)))
    
    conds_3 = np.logical_and(np.logical_and(EVENT_TYPE == 'CHANGE',
                                            EVENT_TYPE.shift(-3) == 'FAC'),
                             np.logical_and(df.game_seconds == df.game_seconds.shift(-3),
                                            df.game_period == df.game_period.shift(-3)))
    
    conds_4 = np.logical_and(np.logical_and(EVENT_TYPE == 'CHANGE',
                                            EVENT_TYPE.shift(-4) == 'FAC'),
                             np.logical_and(df.game_seconds == df.game_seconds.shift(-4),
                                            df.game_period == df.game_period.shift(-4)))
    
    conditions = [conds_1, conds_2, conds_3, conds_4]
    
    values = [df.home_zone.shift(-1),
              df.home_zone.shift(-2),
              df.home_zone.shift(-3),
              df.home_zone.shift(-4)]
    
    df['zone_start'] = np.select(conditions, values, np.nan)
    
    away_zones = {'Off': 'Def', 'Neu': 'Neu', 'Def': 'Off'} # Flipping zones because they're in home zone format
    
    is_away = EVENT_TEAM == AWAY_TEAM
    
    conditions = [np.logical_and(is_away, df.zone_start == 'Off'), np.logical_and(is_away, df.zone_start == 'Def')]
    values = ['Def', 'Off']
    
    #df.zone_start = np.where(EVENT_TEAM == AWAY_TEAM, df.zone_start.map(away_zones).fillna(df.zone_start), df.zone_start)
    
    df.zone_start = np.select(conditions, values, df.zone_start)
    
    df.zone_start = np.where(np.logical_and(EVENT_TYPE == 'CHANGE', pd.isna(df.zone_start)), 'OTF', df.zone_start)
    
    df.zone_start = np.where(df.clock_time == '0:00', np.nan, df.zone_start)
    
    df.zone_start = df.zone_start.str.upper()
    
    df.event_zone = df.event_zone.str.upper()
    
    ## Fixing strength states for changes preceding different strength states
    
    conditions = [conds_1, conds_2, conds_3, conds_4]
    
    values = [df.game_strength_state.shift(-1),
              df.game_strength_state.shift(-2),
              df.game_strength_state.shift(-3),
              df.game_strength_state.shift(-4),]
    
    df.game_strength_state = np.select(conditions, values, df.game_strength_state)
    
    ## Adding strength state & score state
    
    conditions = [EVENT_TEAM == HOME_TEAM, EVENT_TEAM == AWAY_TEAM]
    
    strength_split = df.game_strength_state.str.split('v', expand = True)
    
    values = [df.game_strength_state, strength_split[1] + 'v' + strength_split[0]]
    df['strength_state'] = np.select(conditions, values, np.nan)
    
    values.reverse()
    df['opp_strength_state'] = np.select(conditions, values, np.nan)
    
    df.strength_state = np.where(df.game_strength_state == 'illegal', 'illegal', df.strength_state)
    
    df.opp_strength_state = np.where(df.game_strength_state == 'illegal', 'illegal', df.opp_strength_state)
              
    score_split = df.game_score_state.str.split('v', expand = True)
    
    values = [df.game_score_state, score_split[1] + 'v' + score_split[0]]
    df['score_state'] = np.select(conditions, values, np.nan)
    
    values.reverse()
    df['opp_score_state'] = np.select(conditions, values, np.nan)
    
    ## Swapping faceoff event_players
    
    conditions = np.logical_and(df.event_type == 'FAC', EVENT_TEAM == HOME_TEAM)
    
    df.event_player_1, df.event_player_2 = np.where(conditions, [df.event_player_2, df.event_player_1],
                                                    [df.event_player_1, df.event_player_2])
    
    ## Adding is_home dummy variable
    
    conditions = [df.event_team == df.home_team, df.event_team == df.away_team]
    values = [1, 0]
    
    df['is_home'] = np.select(conditions, values, np.nan)
    
    ## Adding dummy variables 
    
    df = pd.concat([df, pd.get_dummies(df.event_type)], axis = 1)
    
    bad_times = [x * 60 * 20 for x in range(0, 9)]

    max_game_seconds = df.groupby('game_id')['game_seconds'].transform('max')
    
    #conds = np.logical_and.reduce([~df.game_seconds.isin(bad_times),
    #                               df.game_seconds != max_game_seconds,
    #                               np.logical_and.reduce([
    #                                   df.game_period == 4,
    #                                   df.game_seconds == 3600, 
    #                                   df.session == 'R']),
    #                               df.event_type == 'FAC'
    #                              ]
    #                             )
    
    conds = df.event_type == 'FAC'
    
    columns = {'DEF': 'DZF', 'NEU': 'NZF', 'OFF': 'OZF'}
    
    df = df.merge(pd.get_dummies(df[conds].event_zone).rename(columns = columns),
                  how = 'left', left_index = True, right_index = True)
    
    #conds = np.logical_and.reduce([~df.game_seconds.isin(bad_times),
    #                               df.game_seconds != max_game_seconds,
    #                               np.logical_and.reduce([
    #                                   df.game_period == 4,
    #                                   df.game_seconds == 3600, 
    #                                   df.session == 'R']),
    #                               df.event_type == 'CHANGE'
    #                              ]
    #                             )
    
    conds = df.event_type == 'CHANGE'
    
    columns = {'DEF': 'DZS', 'NEU': 'NZS', 'OFF': 'OZS'}
    
    df = df.merge(pd.get_dummies(df[conds].zone_start).rename(columns = columns),
                  how = 'left', left_index = True, right_index = True)

    ## Calculating shots, corsi, & fenwick 
    
    df['CORSI'] = df.GOAL + df.SHOT + df.MISS + df.BLOCK
    
    df['FENWICK'] = df.GOAL + df.SHOT + df.MISS

    df.SHOT = df.GOAL + df.SHOT 
    
    ## Adding penalty columns
    
    is_penalty = df.event_type == 'PENL'
    
    penalty_list = ['0min', '2min', '4min', '5min', '10min']
    conditions = list()
    
    for penalty in penalty_list:
        
        conditions.append(np.logical_and(is_penalty, df.event_detail == penalty))
        
    values = ['PEN0', 'PEN2', 'PEN4', 'PEN5', 'PEN10']
    
    df['penalty_type'] = np.select(conditions, values, np.nan)
    
    df = pd.concat([df, pd.get_dummies(df.penalty_type)], axis = 1)
    
    ## Fixing opening change
    
    conditions = (df.event_type == 'CHANGE') & (df.clock_time == '20:00') & (df.strength_state.str.contains('E'))
    
    df.strength_state = np.where(conditions, df.strength_state.shift(-1), df.strength_state)
    
    df.opp_strength_state = np.where(conditions, df.opp_strength_state.shift(-1), df.opp_strength_state)
    
    df.opp_goalie = np.where(conditions, df.opp_goalie.shift(-1), df.opp_goalie)
    
    df.own_goalie = np.where(conditions, df.own_goalie.shift(-1), df.own_goalie)
    
    # Converting names to plain text
    
    player_cols = [col for col in pbp.columns if
                   ('event_player' in col or 'on_' in col or '_goalie' in col)
                   and ('s_on' not in col)]

    for col in player_cols:

        pbp[col] = pbp[col].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
        
    # Replacing team names with codes that match NHL API
    
    replace_teams = {'S.J': 'SJS', 'N.J': 'NJD', 'T.B': 'TBL', 'L.A': 'LAK'}
    
    for old, new in replace_teams.items():
        
        replace_cols = [col for col in df.columns if '_team' in col] + ['players_on', 'players_off', 'event_description']

        for col in replace_cols:

            df[col] = df[col].str.replace(old, new, regex = False)
            
    df['period_seconds'] = df.game_seconds - ((df.game_period - 1) * 1200)

    df.period_seconds = np.where(np.logical_and(df.game_period == 5, df.session == 'R'), 0, df.period_seconds)
    
    ## Reordering columns
    
    columns = ['season', 'game_id', 'game_date', 'session', 'event_index', 'game_period', 'period_seconds', 'game_seconds', 'clock_time',
               'event_type', 'event_description', 'event_detail', 'event_zone', 'event_team', 'opp_team', 'event_player_1',
               'event_player_2', 'event_player_3', 'event_length', 'coords_x', 'coords_y', 'num_on', 'num_off',
               'players_on', 'players_off', 'event_on_1', 'event_on_2', 'event_on_3', 'event_on_4', 'event_on_5', 'event_on_6', 
               'event_on_7', 'opp_on_1', 'opp_on_2', 'opp_on_3', 'opp_on_4', 'opp_on_5','opp_on_6', 'opp_on_7',  
               'home_goalie', 'away_goalie', 'opp_goalie', 'own_goalie', 'home_team', 'away_team', 'is_home', 'home_skaters',
               'away_skaters', 'home_score', 'away_score', 'game_score_state', 'game_strength_state', 'home_zone',
               'pbp_distance', 'event_distance','event_angle', 'home_zonestart', 'face_index', 'pen_index', 'shift_index',
               'pred_goal', 'zone_start', 'strength_state', 'opp_strength_state', 'score_state',  'opp_score_state',
               'BLOCK', 'CHANGE', 'CORSI', 'FAC', 'FENWICK', 'GIVE', 'GOAL', 'HIT', 'MISS', 'PEN0', 'PEN2', 'PEN4', 'PEN5',
               'PEN10', 'SHOT', 'STOP', 'TAKE', 'OZF', 'NZF', 'DZF', 'OZS', 'NZS', 'DZS', 'OTF']
    
    columns = [x for x in columns if x in df.columns]
    
    df = df[columns].copy()

    columns = {x: x.lower() for x in columns}

    df = df.rename(columns = columns)
    
    return df

## Function to munge the shifts data and create roster
def munge_rosters(shifts):
    
    '''
    Preps roster information from Evolving Hockey shifts data
    Returns a dataframe
    
    Arguments
    
        shifts: Dataframe from the Evolving Hockey shifts query
    
    '''
    
    keep = ['player', 'team_num', 'position', 'game_id', 'season', 'session', 'team']
        
    df = shifts[keep].copy().drop_duplicates()
    
    DUOS = {'SEBASTIAN.AHO': df.position == 'D',
            'COLIN.WHITE': df.season >= 20162017,
            'SEAN.COLLINS': df.season >= 20162017,
            'ALEX.PICARD': df.position != 'D',
            'ERIK.GUSTAFSSON': df.season >= 20152016,
            'MIKKO.LEHTONEN': df.season >= 20202021,
            'NATHAN.SMITH': df.season >= 20212022,
            'DANIIL.TARASOV': df.position == 'G'}
    
    DUOS = [np.logical_and(df.player == player, condition) for player, condition in DUOS.items()]
                
    df.player = df.player.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

    df['EH_ID'] = np.where(np.logical_or.reduce(DUOS), df.player + '2', df.player)
    
    replace_teams = {'S.J': 'SJS', 'N.J': 'NJD', 'T.B': 'TBL', 'L.A': 'LAK'}
    
    df.team = df.team.replace(replace_teams)
    
    return df

## Function to add positions to the play-by-play data
def add_positions(pbp, rosters):
    
    '''
    Adds position information and unique IDs to the play-by-play dataframe
    Used in the prep_pbp function
    Returns a dataframe
    
    Arguments:
        
        pbp: Munged Evolving Hockey play-by-play query
        
        rosters: Munged Evolving Hockey shifts query
    
    '''
    
    pbp = pbp.copy()
    
    rosters = rosters.copy()
    
    player_cols = [col for col in pbp.columns if ('event_player' in col or 'on_' in col) and ('s_on' not in col)]

    for col in player_cols:

        pbp[col] = pbp[col].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')#.replace(EH_REPLACE[year])

        keep_list = ['game_id', 'player', 'EH_ID', 'position']

        left_on = ['game_id', col]

        right_on = ['game_id', 'player']

        pbp = pbp.merge(rosters[keep_list], how = 'left', left_on = left_on, right_on = right_on)

        pbp = pbp.rename(columns = {'position': col + '_pos',
                                    'EH_ID': col + '_id'}).drop('player', axis = 1)
        
    player_groups = ['event', 'opp']

    for player_group in player_groups:

        player_types = {'f': ['L', 'C', 'R'], 'd': ['D'], 'g': ['G']}

        for position_group, positions in player_types.items():

            col = f'{player_group}_on_{position_group}'

            pbp[col] = ''
            
            id_col = f'{player_group}_on_{position_group}_id'

            pbp[id_col] = ''

            player_cols = [f'{player_group}_on_{x}' for x in range(1, 7)]

            for player_col in player_cols:

                cond = pbp[f'{player_col}_pos'].isin(positions)

                pbp[col] = np.where(cond, pbp[col] + pbp[player_col] + '_' , pbp[col])
                
                pbp[id_col] = np.where(cond, pbp[id_col] + pbp[f'{player_col}_id'] + '_' , pbp[id_col])

            pbp[col] = pbp[col].str.split('_')

            pbp[col] = pbp[col].str.replace(r'(^, )', '', regex = True)
            
            pbp[id_col] = pbp[id_col].str.split('_').map(lambda x: ', '.join(sorted(x)))

            pbp[id_col] = pbp[id_col].str.replace(r'(^, )', '', regex = True)
        
    cols = ['season', 'game_id', 'game_date', 'session', 'event_index',
            'game_period', 'game_seconds', 'period_seconds', 'clock_time', 'event_type',
            'event_description', 'event_detail', 'event_zone', 'event_team', 
            'opp_team', 'event_player_1', 'event_player_1_id', 'event_player_1_pos',
            'event_player_2', 'event_player_2_id', 'event_player_2_pos', 'event_player_3',
            'event_player_3_id', 'event_player_3_pos', 'event_length',
            'coords_x', 'coords_y', 'num_on', 'num_off','players_on', 'players_off',
            'event_on_1', 'event_on_1_id', 'event_on_1_pos', 'event_on_2', 'event_on_2_id', 
            'event_on_2_pos', 'event_on_3', 'event_on_3_id', 'event_on_3_pos', 'event_on_4',
            'event_on_4_id', 'event_on_4_pos', 'event_on_5', 'event_on_5_id', 'event_on_5_pos',
            'event_on_6', 'event_on_6_id', 'event_on_6_pos', 'event_on_7', 'event_on_7_id',
            'event_on_7_pos', 'event_on_f', 'event_on_f_id', 'event_on_d', 'event_on_d_id',
            'event_on_g', 'event_on_g_id', 'opp_on_1', 'opp_on_1_id', 'opp_on_1_pos',
            'opp_on_2', 'opp_on_2_id', 'opp_on_2_pos', 'opp_on_3', 'opp_on_3_id',
            'opp_on_3_pos', 'opp_on_4', 'opp_on_4_id', 'opp_on_4_pos', 'opp_on_5',
            'opp_on_5_id', 'opp_on_5_pos', 'opp_on_6', 'opp_on_6_id', 'opp_on_6_pos', 'opp_on_7',
            'opp_on_7_id', 'opp_on_7_pos', 'opp_on_f', 'opp_on_f_id', 'opp_on_d', 'opp_on_d_id', 
            'opp_on_g', 'opp_on_g_id', 'home_goalie', 'away_goalie', 'opp_goalie',
            'own_goalie', 'home_team', 'away_team', 'is_home', 'home_skaters',
            'away_skaters', 'home_score', 'away_score', 'game_score_state',
            'game_strength_state', 'home_zone', 'pbp_distance', 'event_distance',
            'event_angle', 'home_zonestart', 'face_index', 'pen_index',
            'shift_index', 'pred_goal', 'zone_start', 'strength_state',
            'opp_strength_state', 'score_state', 'opp_score_state', 'BLOCK',
            'CHANGE', 'CORSI', 'FAC', 'FENWICK', 'GIVE', 'GOAL', 'HIT',
            'MISS', 'PEN0', 'PEN2', 'PEN4', 'PEN5', 'PEN10', 'SHOT',
            'STOP', 'TAKE', 'OZF', 'NZF', 'DZF', 'OZS', 'NZS', 'DZS', 'OTF']

    cols = [x.lower() for x in cols if x.lower() in pbp.columns]

    pbp = pbp[cols]
        
    return pbp

## Function combining them all to create dataframe
def prep_pbp(pbp, rosters):
    
    '''
    Builds a play-by-play dataframe from Evolving Hockey shifts and play-by-play queries
    Returns a dataframe
    
    Arguments:
        
        pbp: dataframe from the Evolving Hockey play-by-play query
        
        shifts: dataframe from the Evolving Hockey shifts query
    
    '''
    
    pbp = munge_pbp(pbp)
    
    pbp = add_positions(pbp, rosters)
    
    return pbp

## Function to make individual stats dataframe
def prep_ind(df, level = 'game', score = False, teammates = False, opposition = False):
    '''
    Creates individual stats dataframe. Used in game stats function
    '''
    
    ## Filtering out bench minors (not even sure these are in here, tbh)
    
    df = df.copy()

    players = ['event_player_1', 'event_player_2', 'event_player_3']
    
    if level == 'session':
        
        merge_list = ['season', 'session', 'player', 'player_id', 'position', 'team',
                      'strength_state']
        
    if level == 'game':
        
        merge_list = ['season', 'session', 'player', 'player_id', 'position', 'team',
                      'strength_state', 'game_id', 'game_date', 'opp_team',
                     ]
        
    if level == 'period':
        
        merge_list = ['season', 'session', 'player', 'player_id', 'position', 'team',
                      'strength_state', 'game_id', 'game_date', 'opp_team', 'game_period'
                     ]
        
    if score == True:
        
        merge_list.append('score_state')
        
    if teammates == True:
        
        merge_list = merge_list + ['forwards', 'forwards_id',
                                   'defense', 'defense_id',
                                   'own_goalie', 'own_goalie_id']
        
    if opposition == True:
        
        merge_list = merge_list + ['opp_forwards', 'opp_forwards_id',
                                   'opp_defense', 'opp_defense_id',
                                   'opp_goalie', 'opp_goalie_id']

    ind_stats = pd.DataFrame(columns = merge_list)

    for player in players:
        
        player_id = f'{player}_id'
        
        position = f'{player}_pos'
        
        if level == 'session':
        
            group_base = ['season', 'session', 'event_team', player, player_id, position]

        if level == 'game':

            group_base = ['season', 'game_id', 'game_date', 'session', 'event_team', 'opp_team',
                          player, player_id, position]

        if level == 'period':

            group_base = ['season', 'game_id', 'game_date', 'session', 'event_team', 'opp_team',
                          'game_period', player, player_id, position]
        
        mask = df[player] != 'BENCH'

        if player == 'event_player_1':
            
            strength_group = ['strength_state']
            
            if score == True:
                
                score_group = ['score_state']
                
            if teammates == True:
                
                teammates_group = ['event_on_f', 'event_on_f_id',
                                   'event_on_d', 'event_on_d_id',
                                   'event_on_g', 'event_on_g_id']
                
            if opposition == True:
                
                opposition_group = ['opp_on_f', 'opp_on_f_id',
                                    'opp_on_d', 'opp_on_d_id',
                                    'opp_on_g', 'opp_on_g_id']
                
            group_list = group_base + strength_group
            
            if teammates == True:
            
                group_list = group_list + teammates_group

            if score == True:

                group_list = group_list + score_group

            if opposition == True:

                group_list = group_list + opposition_group
            
            stats_list = ['BLOCK', 'FAC', 'GIVE', 'GOAL', 'HIT', 'MISS', 'PEN0', 'PEN2', 'PEN4', 'PEN5', 'PEN10',
                          'SHOT', 'TAKE', 'CORSI', 'FENWICK', 'pred_goal', 'OZF', 'NZF', 'DZF']
            
            stats_dict = {x.lower(): 'sum' for x in stats_list if x.lower() in df.columns}
            
            new_cols = {'BLOCK': 'shots_blocked_off', 'FAC': 'IFOW', 'GIVE': 'GIVE', 'GOAL': 'G', 'HIT': 'iHF',
                        'MISS': 'missed_shots', 'PEN0': 'iPENT0', 'PEN2': 'iPENT2', 'PEN4': 'iPENT4', 'PEN5': 'iPENT5',
                        'PEN10': 'iPENT10', 'SHOT': 'iSF', 'TAKE': 'TAKE', 'CORSI': 'iCF', 'FENWICK': 'iFF',
                        'pred_goal': 'ixG', 'OZF': 'IOZFW', 'NZF': 'INZFW', 'DZF': 'IDZFW', 'event_team': 'team',
                        player: 'player', player_id: 'player_id', position: 'position', 'event_on_f': 'forwards', 
                        'event_on_f_id': 'forwards_id',
                        'event_on_d': 'defense', 'event_on_d_id': 'defense_id', 'event_on_g': 'own_goalie', 'event_on_g_id': 'own_goalie_id',
                        'opp_on_f': 'opp_forwards', 'opp_on_f_id': 'opp_forwards_id', 'opp_on_d': 'opp_defense',
                        'opp_on_d_id': 'opp_defense_id', 'opp_on_g': 'opp_goalie', 'opp_on_g_id': 'opp_goalie_id'
                       }

            new_cols = {k.lower(): v.lower() for k, v in new_cols.items()}

            player_df = df[mask].copy().groupby(group_list, as_index = False).agg(stats_dict)

            player_df = player_df.rename(columns = new_cols)

            #drop_list = [x for x in stats if x not in new_cols.keys() and x in player_df.columns]

        if player == 'event_player_2':

            ## Getting on-ice stats against for player 2
            
            strength_group1 = ['opp_strength_state']
            
            strength_group2 = ['strength_state']
            
            if score == True:
                
                score_group1 = ['opp_score_state']
                
                score_group2 = ['score_state']
                
            if teammates == True:
                
                teammates_group1 = ['opp_on_f', 'opp_on_f_id',
                                   'opp_on_d', 'opp_on_d_id',
                                   'opp_on_g', 'opp_on_g_id']
                
                teammates_group2 = ['event_on_f', 'event_on_f_id',
                                   'event_on_d', 'event_on_d_id',
                                   'event_on_g', 'event_on_g_id']
                
            if opposition == True:
                
                opposition_group1 = ['event_on_f', 'event_on_f_id',
                                    'event_on_d', 'event_on_d_id',
                                    'event_on_g', 'event_on_g_id']
                
                opposition_group2 = ['opp_on_f', 'opp_on_f_id',
                                    'opp_on_d', 'opp_on_d_id',
                                    'opp_on_g', 'opp_on_g_id']
                
            group_list1 = group_base + strength_group1
            
            group_list2 = group_base + strength_group2
            
            if teammates == True:
            
                group_list1 = group_list1 + teammates_group1
                
                group_list2 = group_list2 + teammates_group2

            if score == True:

                group_list1 = group_list1 + score_group1
                
                group_list2 = group_list2 + score_group2

            if opposition == True:

                group_list1 = group_list1 + opposition_group1
                
                group_list2 = group_list2 + opposition_group2
            
            stats_1 = ['BLOCK', 'FAC', 'HIT', 'PEN0', 'PEN2', 'PEN4', 'PEN5', 'PEN10', 'OZF', 'NZF', 'DZF']
            
            stats_1 = {x.lower(): 'sum' for x in stats_1 if x.lower() in df.columns}
    
            new_cols_1 = {'opp_on_g': 'own_goalie', 'opp_on_g_id': 'own_goalie_id', 'event_on_g': 'opp_goalie',
                          'event_on_g_id': 'opp_goalie_id', 'opp_team': 'team', 'event_team': 'opp_team',
                          'opp_score_state': 'score_state', 'opp_strength_state': 'strength_state', 'PEN0': 'iPEND0',
                          'PEN2': 'iPEND2', 'PEN4': 'iPEND4', 'PEN5': 'iPEND5', 'PEN10': 'iPEND10', player: 'player',
                          player_id: 'player_id', position: 'position', 'FAC': 'IFOL', 'HIT': 'iHT',
                          'OZF': 'IOZFL', 'NZF': 'INZFL', 'DZF': 'IDZFL',
                          'opp_on_f': 'forwards', 'opp_on_f_id': 'forwards_id', 'opp_on_d': 'defense',
                          'opp_on_d_id': 'defense_id', 'event_on_f': 'opp_forwards', 'event_on_f_id': 'opp_forwards_id', 'event_on_d': 'opp_defense',
                          'event_on_d_id': 'opp_defense_id'}

            new_cols_1 = {k.lower(): v.lower() for k, v in new_cols_1.items()}

            event_types = ['BLOCK', 'FAC', 'HIT', 'PENL']
        
            mask_1 = np.logical_and(df[player] != 'BENCH', df.event_type.isin(event_types))
            
            opps = df[mask_1].copy().groupby(group_list1, as_index = False).agg(stats_1).rename(columns = new_cols_1)
            
            ## Getting primary assists and primary assists xG from player 2
            
            stats_2 = ['GOAL', 'pred_goal']
            
            stats_2 = {x.lower(): 'sum' for x in stats_2 if x.lower() in df.columns}
            
            new_cols_2 = {'event_team': 'team', player: 'player', player_id: 'player_id', 'GOAL': 'A1', 'pred_goal': 'A1_xG',
                          position: 'position', 'event_on_f': 'forwards', 'event_on_f_id': 'forwards_id',
                          'event_on_d': 'defense', 'event_on_d_id': 'defense_id', 'event_on_g': 'own_goalie', 'event_on_g_id': 'own_goalie_id',
                          'opp_on_f': 'opp_forwards', 'opp_on_f_id': 'opp_forwards_id', 'opp_on_d': 'opp_defense',
                          'opp_on_d_id': 'opp_defense_id', 'opp_on_g': 'opp_goalie', 'opp_on_g_id': 'opp_goalie_id'}

            new_cols_2 = {k.lower(): v.lower() for k, v in new_cols_2.items()}
            
            mask_2 = np.logical_and(df[player] != 'BENCH', df.event_type.isin([x.upper() for x in stats_2.keys()]))
            
            own = df[mask_2].copy().groupby(group_list2, as_index = False).agg(stats_2).rename(columns = new_cols_2)

            player_df = opps.merge(own, left_on = merge_list, right_on = merge_list, how = 'outer').fillna(0)

        if player == 'event_player_3':
            
            group_list = group_base + strength_group
            
            if teammates == True:
            
                group_list = group_list + teammates_group

            if score == True:

                group_list = group_list + score_group

            if opposition == True:

                group_list = group_list + opposition_group
            
            stats_list = ['GOAL', 'pred_goal']
            
            stats_dict = {x.lower(): 'sum' for x in stats_list if x.lower() in df.columns}
        
            player_df = df[mask].groupby(group_list, as_index = False).agg(stats_dict)

            new_cols = {'GOAL': 'A2', 'pred_goal': 'A2_xG', 'event_team': 'team',
                        player: 'player', player_id: 'player_id', position: 'position',
                        'event_on_f': 'forwards', 'event_on_f_id': 'forwards_id',
                        'event_on_d': 'defense', 'event_on_d_id': 'defense_id', 'event_on_g': 'own_goalie',
                        'event_on_g_id': 'own_goalie_id', 'opp_on_f': 'opp_forwards',
                        'opp_on_f_id': 'opp_forwards_id', 'opp_on_d': 'opp_defense',
                        'opp_on_d_id': 'opp_defense_id', 'opp_on_g': 'opp_goalie',
                        'opp_on_g_id': 'opp_goalie_id'}

            new_cols = {k.lower(): v.lower() for k, v in new_cols.items()}
            
            player_df = player_df.rename(columns = new_cols)
        
        ind_stats = ind_stats.merge(player_df, on = merge_list, how = 'outer').fillna(0)
        
    ## Fixing some stats
    
    ind_stats['gax'] = ind_stats.g - ind_stats.ixg
    
    columns = ['season', 'session', 'game_id', 'game_date', 'player', 'player_id', 'position', 
               'team', 'opp_team', 'game_period', 'strength_state', 'score_state', 'opp_goalie',
               'opp_goalie_id', 'own_goalie', 'own_goalie_id', 'forwards', 'forwards_id', 'defense',
               'defense_id', 'opp_forwards', 'opp_forwards_id', 'opp_defense', 'opp_defense_id',
               'G', 'A1', 'A2','iSF', 'iFF', 'iCF', 'ixG', 'GaX', 'missed_shots', 'shots_blocked_off',
               'GIVE', 'TAKE', 'iHF', 'iHT', 'IFOW', 'IFOL', 'IOZFW', 'IOZFL', 'INZFW', 'INZFL', 'IDZFW',
               'IDZFL', 'A1_xG', 'A2_xG', 'iPENT0', 'iPENT2', 'iPENT4', 'iPENT5', 'iPENT10', 'iPEND0',
               'iPEND2', 'iPEND4', 'iPEND5', 'iPEND10']
    
    columns = [x.lower() for x in columns if x.lower() in ind_stats.columns]
    
    ind_stats = ind_stats[columns]
    
    stats = ['G', 'A1', 'A2','iSF', 'iFF', 'iCF', 'ixG', 'GaX', 'missed_shots', 'shots_blocked_off',
             'GIVE', 'TAKE', 'iHF', 'iHT', 'IFOW', 'IFOL', 'A1_xG', 'A2_xG', 'iPENT0', 'iPENT2', 'iPENT4',
             'iPENT5', 'iPENT10', 'iPEND0', 'iPEND2', 'iPEND4', 'iPEND5', 'iPEND10', 'IOZFW', 'IOZFL',
             'INZFW', 'INZFL', 'IDZFW', 'IDZFL',]
    
    stats = [x.lower() for x in stats if x.lower() in ind_stats.columns]
    
    ind_stats = ind_stats.loc[(ind_stats[stats]!=0).any(axis=1)]
        
    return ind_stats

## Function to prep the on-ice stats
def prep_oi(data, level = 'game', score = False, teammates = False, opposition = False):
    
    '''
    
    Function to prep forward line combinations or defensive pairings
    dataframe from Evolving Hockey play-by-play data created using
    the chickenstats python package
    
    Takes play-by-play dataframe and aggregates to desired level, either game or period
    Can include score state within aggregation if option is True
    Can include aggregation based on teammates if True
    Can include aggregation based on opposition if true
    
    Arguments
    
        data: DataFrame
        
        position: forwards (f) or defense (d)
        
        level: session, game or period, default 'game'
        
        score: True or False, default False
        
        teammates: True or False, default False
        
        opposition: True or False, default False
        
    '''
    
    
    df = data.copy()
    
    stats_list = ['BLOCK', 'GOAL', 'HIT', 'MISS', 'PEN0', 'PEN2', 'PEN4', 'PEN5', 'PEN10',
                  'SHOT', 'CORSI', 'FENWICK', 'pred_goal', 'FAC', 'OFF', 'DEF', 'NEU', 'OZF', 'DZF', 'NZF',
                  'event_length']
            
    stats_dict = {x.lower(): 'sum' for x in stats_list if x.lower() in df.columns}

    players = [f'event_on_{x}' for x in range(1, 8)] + [f'opp_on_{x}' for x in range(1, 8)]

    event_list = []

    opp_list = []

    for player in players:
        
        position = f'{player}_pos'
        
        player_id = f'{player}_id'
        
        if level == 'session':
        
            group_list = ['season', 'session']

        if level == 'game':

            group_list = ['season', 'game_id', 'game_date', 'session', 'event_team', 'opp_team']

        if level == 'period':

            group_list = ['season', 'game_id', 'game_date', 'session', 'event_team', 'opp_team',
                          'game_period']

        ## Accounting for desired player
        
        if 'event_on' in player:
            
            if level == 'session':
                
                group_list.append('event_team')
            
            strength_group = ['strength_state']
            
            teammates_group = ['event_on_f', 'event_on_f_id',
                               'event_on_d', 'event_on_d_id',
                               'event_on_g', 'event_on_g_id']
            
            score_group = ['score_state']
            
            opposition_group = ['opp_on_f', 'opp_on_f_id',
                                'opp_on_d', 'opp_on_d_id',
                                'opp_on_g', 'opp_on_g_id'] 
            
            col_names = {'event_team': 'team', player: 'player', player_id: 'player_id',
                         position: 'position', 'GOAL': 'GF', 'HIT': 'HF', 'MISS': 'MSF',
                         'BLOCK': 'BSF', 'PEN0': 'PENT0', 'PEN2': 'PENT2', 'PEN4': 'PENT4',
                         'PEN5': 'PENT5', 'PEN10': 'PENT10', 'CORSI': 'CF', 'FENWICK': 'FF',
                         'pred_goal': 'xGF', 'FAC': 'FOW', 'OZF': 'OZFW', 'DZF': 'DZFW', 'NZF': 'NZFW',
                         'SHOT': 'SF', 'event_on_f': 'forwards', 'event_on_f_id': 'forwards_id',
                         'event_on_d': 'defense', 'event_on_d_id': 'defense_id', 'event_on_g': 'own_goalie',
                         'event_on_g_id': 'own_goalie_id', 'opp_on_f': 'opp_forwards', 
                         'opp_on_f_id': 'opp_forwards_id', 'opp_on_d': 'opp_defense',
                         'opp_on_d_id': 'opp_defense_id', 'opp_on_g': 'opp_goalie', 
                         'opp_on_g_id': 'opp_goalie_id', 
                        }

            col_names = {k.lower(): v.lower() for k, v in col_names.items()}
            
        if 'opp_on' in player:
            
            if level == 'session':
                
                group_list.append('opp_team')
            
            strength_group = ['opp_strength_state']
            
            teammates_group = ['opp_on_f', 'opp_on_f_id',
                               'opp_on_d', 'opp_on_d_id',
                               'opp_on_g', 'opp_on_g_id'] 
            
            score_group = ['opp_score_state']
            
            opposition_group = ['event_on_f', 'event_on_f_id',
                                'event_on_d', 'event_on_d_id',
                                'event_on_g', 'event_on_g_id']
            
            col_names = {'opp_team': 'team', 'event_team': 'opp_team', 'opp_goalie': 'own_goalie',
                         'own_goalie': 'opp_goalie', 'opp_score_state': 'score_state',
                         'opp_strength_state': 'strength_state', player: 'player', player_id: 'player_id', 
                         position: 'position', 'BLOCK': 'BSA', 'GOAL': 'GA', 'HIT': 'HT', 'MISS': 'MSA',
                         'PEN0': 'PEND0', 'PEN2': 'PEND2', 'PEN4': 'PEND4', 'PEN5': 'PEND5', 'PEN10': 'PEND10',
                         'SHOT': 'SA', 'CORSI': 'CA', 'FENWICK': 'FA', 'pred_goal': 'xGA', 'FAC': 'FOL', 'OZF': 'DZFL',
                         'DZF': 'OZFL', 'NZF': 'NZFL', 'event_on_f': 'opp_forwards', 'event_on_f_id': 'opp_forwards_id',
                         'event_on_d': 'opp_defense', 'event_on_d_id': 'opp_defense_id', 'event_on_g': 'opp_goalie',
                         'event_on_g_id': 'opp_goalie_id', 'opp_on_f': 'forwards', 'opp_on_f_id': 'forwards_id',
                         'opp_on_d': 'defense', 'opp_on_d_id': 'defense_id', 'opp_on_g': 'own_goalie', 
                         'opp_on_g_id': 'own_goalie_id', 
                        }
            
            col_names = {k.lower(): v.lower() for k, v in col_names.items()}

        group_list = group_list + [player, player_id, position] + strength_group
        
        if teammates == True:
            
            group_list = group_list + teammates_group
            
        if score == True:
            
            group_list = group_list + score_group
            
        if opposition == True:
            
            group_list = group_list + opposition_group
            
        player_df = df.groupby(group_list, as_index = False).agg(stats_dict)
        
        col_names = {key: value for key, value in col_names.items() if key in player_df.columns}
            
        player_df = player_df.rename(columns = col_names)

        if 'event_on' in player:
        
            event_list.append(player_df)

        else:
            
            opp_list.append(player_df)
    
    ## On-ice stats
    
    merge_cols = ['season', 'session', 'game_id', 'game_date', 'team', 'opp_team', 'player',
                  'player_id', 'position', 'game_period', 'strength_state', 'score_state',
                  'opp_goalie', 'opp_goalie_id', 'own_goalie', 'own_goalie_id',
                  'forwards', 'forwards_id', 'defense', 'defense_id', 'opp_forwards',
                  'opp_forwards_id', 'opp_defense', 'opp_defense_id',
                 ]
    
    event_stats = pd.concat(event_list, ignore_index = True)
    
    stats_dict = {x: 'sum' for x in event_stats.columns if x not in merge_cols}
    
    group_list = [x for x in merge_cols if x in event_stats.columns]
    
    event_stats = event_stats.groupby(group_list, as_index = False).agg(stats_dict)
    
    opp_stats = pd.concat(opp_list, ignore_index = True)
    
    stats_dict = {x: 'sum' for x in opp_stats.columns if x not in merge_cols}
    
    group_list = [x for x in merge_cols if x in opp_stats.columns]
    
    opp_stats = opp_stats.groupby(group_list, as_index = False).agg(stats_dict)
    
    merge_cols = [x for x in merge_cols if x in event_stats.columns and x in opp_stats.columns]
        
    oi_stats = event_stats.merge(opp_stats, on = merge_cols, how = 'outer').fillna(0)
    
    oi_stats['toi'] = (oi_stats.event_length_x + oi_stats.event_length_y) / 60
    
    oi_stats = oi_stats.drop(['event_length_x', 'event_length_y'], axis = 1)
    
    fo_list = ['ozf', 'dzf', 'nzf']
    
    for fo in fo_list:
        
        oi_stats[fo] = oi_stats[f'{fo}w'] + oi_stats[f'{fo}l']
        
    oi_stats['fac'] = oi_stats.ozf + oi_stats.nzf + oi_stats.dzf
        
    columns = ['season', 'session', 'game_id', 'game_date', 'player', 'player_id', 'position', 
               'team', 'opp_team', 'game_period', 'strength_state', 'score_state', 'opp_goalie',
               'opp_goalie_id', 'own_goalie', 'own_goalie_id', 'forwards', 'forwards_id', 'defense',
               'defense_id', 'opp_forwards', 'opp_forwards_id', 'opp_defense', 'opp_defense_id',
               'TOI', 'GF', 'SF', 'FF', 'CF', 'xGF', 'BSF', 'MSF', 'GA', 'SA', 'FA', 'CA', 'xGA',
               'BSA', 'MSA', 'HF', 'HT', 'OZF', 'NZF', 'DZF', 'FOW', 'FOL', 'OZFW', 'OZFL', 'NZFW', 'NZFL', 'DZFW',
               'DZFL', 'PENT0', 'PENT2', 'PENT4', 'PENT5', 'PENT10', 'PEND0', 'PEND2', 'PEND4',
               'PEND5', 'PEND10',
              ]
    
    columns = [x.lower() for x in columns if x.lower() in oi_stats.columns]
    
    oi_stats = oi_stats[columns]
    
    stats = ['TOI', 'GF', 'SF', 'FF', 'CF', 'xGF', 'BSF', 'MSF', 'GA', 'SA', 'FA', 'CA', 'xGA',
             'BSA', 'MSA', 'HF', 'HT', 'OZF', 'NZF', 'DZF', 'FOW', 'FOL', 'OZFW', 'OZFL', 'NZFW', 
             'NZFL', 'DZFW', 'DZFL', 'PENT0', 'PENT2', 'PENT4', 'PENT5', 'PENT10', 'PEND0', 
             'PEND2', 'PEND4', 'PEND5', 'PEND10',]
    
    stats = [x.lower() for x in stats if x.lower() in oi_stats.columns]
    
    oi_stats = oi_stats.loc[(oi_stats[stats]!=0).any(axis=1)]
    
    return oi_stats

## Function combining the on-ice and individual stats
def prep_stats(df, level = 'game', score = False, teammates = False, opposition = False):
    
    ind = prep_ind(df, level, score, teammates, opposition)
    
    oi = prep_oi(df, level, score, teammates, opposition)
    
    merge_cols = ['season', 'session', 'game_id', 'game_date', 'player', 'player_id', 'position', 'team',
                  'opp_team', 'strength_state', 'score_state', 'game_period', 'opp_goalie', 'opp_goalie_id', 'own_goalie', 
                  'own_goalie_id', 'forwards', 'forwards_id', 'defense', 'defense_id', 'opp_forwards', 
                  'opp_forwards_id', 'opp_defense', 'opp_defense_id']
    
    stats = oi.merge(ind, how = 'left', left_on = merge_cols, right_on = merge_cols).fillna(0)

    stats = stats.loc[stats.toi > 0].reset_index(drop = True).copy()
    
    stats_list = [x for x in stats.columns if x not in merge_cols]
    
    for stat in stats_list:
        
        stats[stat] = stats[stat].fillna(0)
        
    return stats

## Function to prep the lines data
def prep_lines(data, position, level = 'game', score = False, teammates = False, opposition = False):
    
    '''
    Function to prep forward line combinations or defensive pairings
    dataframe from Evolving Hockey play-by-play data created using
    the chickenstats python package
    
    Takes play-by-play dataframe and aggregates to desired level, either game or period
    Can include score state within aggregation if option is True
    Can include aggregation based on teammates if True
    Can include aggregation based on opposition if true
    
    Arguments
    
        data: DataFrame
        
        position: forwards (f) or defense (d)
        
        level: game or period, default 'game'
        
        score: True or False, default False
        
        teammates: True or False, default False
        
        opposition: True or False, default False
        
    '''
    
    # Creating the "for" dataframe
    
    ## Accounting for desired level of aggregation
    
    if level == 'session':
        
        group_base = ['season', 'session', 'event_team', 'strength_state']
    
    if level == 'game':
        
        group_base = ['season', 'game_id', 'game_date', 'session', 'event_team', 'opp_team',
                      'strength_state']
        
    if level == 'period':
        
        group_base = ['season', 'game_id', 'game_date', 'session', 'event_team', 'opp_team',
                      'game_period', 'strength_state']
        
    ## Accounting for score state
        
    if score == True:
        
        group_list = group_base + ['score_state']
        
    ## Accounting for desired position 
        
    group_list = group_list + [f'event_on_{position}', f'event_on_{position}_id']
    
    ## Accounting for teammates
    
    if teammates == True:
        
        if position == 'f':
            
            group_list = group_list + ['event_on_d', 'event_on_d_id', 'event_on_g', 'event_on_g_id']
            
        if position == 'd':
            
            group_list = group_list + ['event_on_f', 'event_on_f_id', 'event_on_g', 'event_on_g_id']
            
    ## Accounting for opposition
            
    if opposition == True:
        
        group_list = group_list + ['opp_on_f', 'opp_on_f_id',
                                   'opp_on_d', 'opp_on_d_id',
                                   'opp_on_g', 'opp_on_g_id']            
    
    ## Creating dictionary of statistics for the groupby function
    
    stats = ['pred_goal', 'CORSI', 'FENWICK', 'GOAL', 'MISS', 'SHOT', 'event_length',
             'FAC', 'OZF', 'NZF', 'DZF', 'HIT', 'GIVE', 'TAKE', 'PEN0', 'PEN2', 'PEN4',
             'PEN5', 'PEN10'
             ]

    stats = {x.lower(): 'sum' for x in stats if x.lower() in data.columns}
        
    ## Aggregating the "for" dataframe
        
    lines_f = data.groupby(group_list, as_index = False, dropna = False).agg(stats)
    
    ## Creating the dictionary to change column names

    columns = ['xGF', 'CF', 'FF', 'GF', 'MSF', 'SF', 'TOI', 'FOW', 'OZFW', 'NZFW', 'DZFW',
    'HF', 'GIVE', 'TAKE', 'PENT0', 'PENT2', 'PENT4', 'PENT5', 'PENT10',]

    columns = [x.lower() for x in columns]

    columns = dict(zip(stats, columns))
    
    ## Accounting for positions
    
    columns.update({'event_on_f': 'forwards', 'event_on_f_id': 'forwards_id', 'event_team': 'team',
                    'event_on_d': 'defense', 'event_on_d_id': 'defense_id',
                    'event_on_g': 'own_goalie', 'event_on_g_id': 'own_goalie_id',
                    'opp_on_f': 'opp_forwards', 'opp_on_f_id': 'opp_forwards_id', 
                    'opp_on_d': 'opp_defense', 'opp_on_d_id': 'opp_defense_id',
                    'opp_on_g': 'opp_goalie', 'opp_on_g_id': 'opp_goalie_id'
                   })
    
    #columns = {k: v for k, v in columns.items() if k in lines_f.columns}
    
    lines_f = lines_f.rename(columns = columns)
    
    cols = ['forwards', 'forwards_id',
            'defense', 'defense_id',
            'own_goalie', 'own_goalie_id',
            'opp_forwards', 'opp_forwards_id',
            'opp_defense', 'opp_defense_id',
            'opp_goalie', 'opp_goalie_id',
           ]
    
    cols = [x for x in cols if x in lines_f]
    
    for col in cols:
        
        lines_f[col] = lines_f[col].fillna('EMPTY')
    
    # Creating the against dataframe
        
    ## Accounting for desired level of aggregation
    
    if level == 'session':
        
        group_base = ['season', 'session', 'opp_team', 'opp_strength_state']
    
    if level == 'game':
        
        group_base = ['season', 'game_id', 'game_date', 'session', 'event_team', 'opp_team',
                      'opp_strength_state']
        
    if level == 'period':
        
        group_base = ['season', 'game_id', 'game_date', 'session', 'event_team', 'opp_team',
                      'game_period', 'opp_strength_state']
        
    ## Accounting for score state
        
    if score == True:
        
        group_list = group_base + ['opp_score_state']
        
    ## Accounting for desired position 
        
    group_list = group_list + [f'opp_on_{position}', f'opp_on_{position}_id']
    
    ## Accounting for teammates
    
    if teammates == True:
        
        if position == 'f':
            
            group_list = group_list + ['opp_on_d', 'opp_on_d_id', 'opp_on_g', 'opp_on_g_id']
            
        if position == 'd':
            
            group_list = group_list + ['opp_on_f', 'opp_on_f_id', 'opp_on_g', 'opp_on_g_id']
            
    ## Accounting for opposition
            
    if opposition == True:
        
        group_list = group_list + ['event_on_f', 'event_on_f_id',
                                   'event_on_d', 'event_on_d_id',
                                   'event_on_g', 'event_on_g_id'] 
    
    ## Creating dictionary of statistics for the groupby function
    
    stats = ['pred_goal', 'CORSI', 'FENWICK', 'GOAL', 'MISS', 'SHOT', 'event_length', 'FAC', 'OZF',
             'NZF', 'DZF', 'HIT', 'PEN0', 'PEN2', 'PEN4', 'PEN5', 'PEN10']

    stats = {x.lower(): 'sum' for x in stats if x.lower() in data.columns}
        
    ## Aggregating "aggainst" dataframe

    lines_a = data.groupby(group_list, as_index = False, dropna = False).agg(stats)
    
    ## Creating the dictionary to change column names

    columns = ['xGA', 'CA', 'FA', 'GA', 'MSA', 'SA', 'TOI', 'FOL', 'OZFL', 'NZFL', 'DZFL', 'HT',
                'PEND0', 'PEND2', 'PEND4', 'PEND5', 'PEND10',]

    columns = [x.lower() for x in columns]

    columns = dict(zip(stats, columns))
    
    ## Accounting for positions
        
    columns.update({'opp_team': 'team', 'event_team': 'opp_team', 'opp_on_f': 'forwards',
                    'opp_on_f_id': 'forwards_id', 'opp_strength_state': 'strength_state',
                    'opp_on_d': 'defense', 'opp_on_d_id': 'defense_id',
                    'event_on_f': 'opp_forwards', 'event_on_f_id': 'opp_forwards_id', 
                    'event_on_d': 'opp_defense', 'event_on_d_id': 'opp_defense_id', 
                    'opp_score_state': 'score_state', 'event_on_g': 'opp_goalie',
                    'event_on_g_id': 'opp_goalie_id', 'opp_on_g': 'own_goalie', 
                    'opp_on_g_id': 'own_goalie_id',
                   })
        
    #columns = {k: v for k, v in columns.items() if k in lines_a.columns}
    
    lines_a = lines_a.rename(columns = columns)
    
    cols = ['forwards', 'forwards_id',
            'defense', 'defense_id',
            'own_goalie', 'own_goalie_id',
            'opp_forwards', 'opp_forwards_id',
            'opp_defense', 'opp_defense_id',
            'opp_goalie', 'opp_goalie_id'
           ]
    
    cols = [x for x in cols if x in lines_a]
    
    for col in cols:
        
        lines_a[col] = lines_a[col].fillna('EMPTY')
    
    # Merging the "for" and "against" dataframes
    
    if level == 'session':
        
        if position == 'f':

            merge_list = ['season', 'session', 'team', 'strength_state',
                          'forwards', 'forwards_id',]

        if position == 'd':

            merge_list = ['season', 'session', 'team', 'strength_state',
                          'defense', 'defense_id',]
    
    if level == 'game':
    
        if position == 'f':

            merge_list = ['season', 'game_id', 'game_date', 'session', 'team', 'opp_team', 'strength_state',
                          'forwards', 'forwards_id',]

        if position == 'd':

            merge_list = ['season', 'game_id', 'game_date', 'session', 'team', 'opp_team', 'strength_state',
                          'defense', 'defense_id',]
        
    if level == 'period':
        
        if position == 'f':

            merge_list = ['season', 'game_id', 'game_date', 'session', 'team', 'opp_team', 'strength_state',
                          'forwards', 'forwards_id', 'game_period']

        if position == 'd':

            merge_list = ['season', 'game_id', 'game_date', 'session', 'team', 'opp_team', 'strength_state',
                          'defense', 'defense_id', 'game_period']
        
    if score == True:
        
        merge_list.append('score_state')
        
    if teammates == True:
        
        if position == 'f': 
        
            merge_list = merge_list + ['defense', 'defense_id', 'own_goalie', 'own_goalie_id',]
            
        if position == 'd': 
        
            merge_list = merge_list + ['forwards', 'forwards_id', 'own_goalie', 'own_goalie_id',]
            
    if opposition == True:
        
        merge_list = merge_list + ['opp_forwards', 'opp_forwards_id',
                                   'opp_defense', 'opp_defense_id',
                                   'opp_goalie', 'opp_goalie_id',
                                  ]
        
    lines = lines_f.merge(lines_a, how = 'outer', on = merge_list, suffixes = ('_x', '')).fillna(0)
    
    cols = ['forwards', 'forwards_id',
            'defense', 'defense_id',
            'own_goalie', 'own_goalie_id',
            'opp_forwards', 'opp_forwards_id',
            'opp_defense', 'opp_defense_id',
            'opp_goalie', 'opp_goalie_id'
           ]
    
    cols = [x for x in cols if x in lines]
    
    for col in cols:
        
        lines[col] = lines[col].fillna('EMPTY')

    lines.toi = lines.toi_x + lines.toi

    lines = lines.drop(columns = 'toi_x')
    
    lines['ozf'] = lines.ozfw + lines.ozfl

    lines['nzf'] = lines.nzfw + lines.nzfl

    lines['dzf'] = lines.dzfw + lines.dzfl
    
    stats_f = ['CF', 'FF', 'SF', 'GF', 'xGF']

    stats_f = [x.lower() for x in stats_f]

    stats_a = ['CA', 'FA', 'SA', 'GA', 'xGA']

    stats_a = [x.lower() for x in stats_a]

    #for stat_f, stat_a in dict(zip(stats_f, stats_a)).items():
    
    #    lines[f'{stat_f.lower()}_perc'] = (lines[stat_f] / (lines[stat_f] + lines[stat_a])).fillna(0)

    lines = lines.loc[lines.toi > 0].reset_index(drop = True).copy()
        
    return lines

## Function to prep the team stats
def prep_team(data, level = 'game', strengths = True, score = False):
    '''
    Function to prep team stats from pbp dataframe
    
    Arguments:
        data: pbp dataframe from evolving hockey
        level: can be game or season
    '''
    
    ## Getting the "for" stats
    
    group_list = ['season', 'session', 'event_team']

    if strengths == True:

        group_list.append('strength_state')
    
    if level == 'game':
        
        group_list.insert(3, 'opp_team')
        
        group_list[2:2] = ['game_id', 'game_date']
        
    if score == True:
        
        group_list.append('score_state')
        
    agg_stats = ['pred_goal', 'shot', 'miss', 'block', 'corsi',
                 'fenwick', 'goal', 'give', 'take',
                 'hit', 'pen0', 'pen2', 'pen4', 'pen5',
                 'pen10', 'fac', 'ozf', 'nzf', 'dzf','event_length']
    
    agg_dict = {x: 'sum' for x in agg_stats if x in data.columns}
    
    new_cols = ['xgf', 'sf', 'msf', 'bsa', 'cf', 'ff', 
                'gf', 'give', 'take', 'hf', 'pent0', 'pent2',
                'pent4', 'pent5', 'pent10', 'fow', 'ozfw', 'nzfw',
                'dzfw', 'toi']
    
    new_cols = dict(zip(agg_stats, new_cols))
    
    new_cols.update({'event_team': 'team'})
    
    stats_for = data.groupby(group_list, as_index = False).agg(agg_dict).rename(columns = new_cols)
    
    ## Getting the "against" stats
    
    group_list = ['season', 'session', 'opp_team']

    if strengths == True:

        group_list.append('opp_strength_state')
    
    if level == 'game':
        
        group_list.insert(3, 'event_team')
        
        group_list[2:2] = ['game_id', 'game_date']
        
    if score == True:
        
        group_list.append('opp_score_state')
        
    agg_stats = ['pred_goal', 'shot', 'miss', 'block', 'corsi',
                 'fenwick', 'goal', #'give', 'take',
                 'hit', 'pen0', 'pen2', 'pen4', 'pen5',
                 'pen10', 'fac', 'ozf', 'nzf', 'dzf','event_length']
    
    agg_dict = {x: 'sum' for x in agg_stats if x in data.columns}
    
    new_cols = ['xga', 'sa', 'msa', 'bsf', 'ca', 'fa', 
                'ga', #'give', 'take',
                'ht', 'pend0', 'pend2',
                'pend4', 'pend5', 'pend10', 'fol', 'ozfl', 'nzfl',
                'dzfl', 'toi'
               ]
    
    new_cols = dict(zip(agg_stats, new_cols))
    
    new_cols.update({'opp_team': 'team',
                     'opp_score_state': 'score_state',
                     'opp_strength_state': 'strength_state',
                     'event_team': 'opp_team',
                    })
    
    stats_against = data.groupby(group_list, as_index = False).agg(agg_dict).rename(columns = new_cols)
    
    merge_list = ['season', 'session', 'game_id', 'game_date',
                  'team', 'opp_team', 'strength_state', 'score_state']
    
    merge_list = [x for x in merge_list if x in stats_for]
    
    team_stats = stats_for.merge(stats_against, on = merge_list, how = 'outer')
    
    team_stats['toi'] = (team_stats.toi_x + team_stats.toi_y) / 60
    
    team_stats = team_stats.drop(['toi_x', 'toi_y'], axis = 1)
    
    fos = ['ozf', 'nzf', 'dzf']
    
    for fo in fos:
        
        team_stats[fo] = team_stats[f'{fo}w'] + team_stats[f'{fo}w']
        
    team_stats = team_stats.dropna(subset = 'toi')
    
    cols = ['season', 'session', 'game_id', 'game_date', 'team', 'opp_team',
            'strength_state', 'score_state', 'toi', 'xgf', 'xga', 'gf', 'ga', 'sf', 'sa',
            'msf', 'msa', 'bsf', 'bsa', 'cf', 'ca', 'ff', 'fa', 'give',
            'take', 'hf', 'ht', 'pent0', 'pent2', 'pent4', 'pent5', 'pent10',
            'fow', 'ozfw', 'nzfw', 'dzfw', 'pend0', 'pend2', 'pend4', 'pend5',
            'pend10', 'fol', 'ozfl', 'nzfl', 'dzfl', 'ozf', 'nzf', 'dzf']
    
    cols = [x for x in cols if x in team_stats]
    
    team_stats = team_stats[cols]
    
    return team_stats