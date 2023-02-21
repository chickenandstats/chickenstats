def api_events_fixes(game_id, api_events):
    '''
    
    This is used for fixing API events errors

    Known errors that have no fix:

    2021020562 | CHL at 2898 game seconds is not in API events feed
    2021020767 | CHL at 3598 game seconds is not in API events feed
    2021020882 | SHOT at 249, 1785, & 1786 game seconds are not in API events feed
    2021020894 | SHOT by Boldy at 3507 game seconds is not in API events feed



    '''

    if game_id == 2019020144:

        takes = {x['game_seconds']: x for x in api_events if x['event'] == 'TAKE'}

        bad_takes = {1768: {'player_1': 'JAKE GUENTZEL', 'player_1_eh_id': 'JAKE.GUENTZEL', 'player_1_api_id': 8477404},}

        for game_seconds, new_values in bad_takes.items():

            take = takes.get(game_seconds)

            if take is not None:

                take.update(new_values)

    if game_id == 2019020215:

        bad_faceoffs = [1334, 1864, 2119]

        bad_faceoffs = [x for x in api_events if (x['game_seconds'] in bad_faceoffs and x['event'] == 'FAC')]

        for bad_faceoff in bad_faceoffs:

            if bad_faceoff['event_team'] == 'VGK':

                event_team = 'WPG'

                event_team_name = 'WINNIPEG JETS'

            elif bad_faceoff['event_team'] == 'WPG':

                event_team = 'VGK'

                event_team_name = 'VEGAS GOLDEN KNIGHTS'

            new_values = {'event_team': event_team,
                            'event_team_name': event_team_name,
                            'player_1': bad_faceoff['player_2'],
                            'player_1_api_id': bad_faceoff['player_2_api_id'],
                            'player_1_eh_id': bad_faceoff['player_2_eh_id'],
                            'player_2': bad_faceoff['player_1'],
                            'player_2_api_id': bad_faceoff['player_1_api_id'],
                            'player_2_eh_id': bad_faceoff['player_1_eh_id'],}

            bad_faceoff.update(new_values)

    if game_id == 2019020289:

        bad_idxs = {255: {'player_1': 'NICK SCHMALTZ', 'player_1_api_id': 8477951,
                            'player_1_eh_id': 'NICK.SCHMALTZ',}}

        bad_events = {x['event_idx']: x for x in api_events if x['event_idx'] in bad_idxs.keys()}

        for idx, new_values in bad_idxs.items():

            event = bad_events.get(idx)

            if event is not None:

                event.update(new_values)

    if game_id == 2019020416:

        bad_idxs = {124: {'player_1': 'LUKE KUNIN', 'player_1_api_id': 8479316,
                            'player_1_eh_id': 'LUKE.KUNIN', 'player_2': "ROOPE HINTZ",
                            'player_2_api_id': 8478449, 'player_2_eh_id': "ROOPE.HINTZ"},
                    131: {'player_1': 'ERIC STAAL', 'player_1_api_id': 8470595,
                            'player_1_eh_id': 'ERIC.STAAL',},}

        bad_events = {x['event_idx']: x for x in api_events if x['event_idx'] in bad_idxs.keys()}

        for idx, new_values in bad_idxs.items():

            event = bad_events.get(idx)

            if event is not None:

                event.update(new_values)

    if game_id == 2019020417:

        bad_idxs = {6: {'player_1': 'NICK SUZUKI', 'player_1_api_id': 8480018,
                            'player_1_eh_id': 'NICK.SUZUKI',},}

        bad_events = {x['event_idx']: x for x in api_events if x['event_idx'] in bad_idxs.keys()}

        for idx, new_values in bad_idxs.items():

            event = bad_events.get(idx)

            if event is not None:

                event.update(new_values)

    if game_id == 2019020779:

        bad_idxs = {26: {'period_seconds': 301, 'period_time': '5:01', 'game_seconds': 301,
                            'player_1': 'NIKLAS HJALMARSSON', 'player_1_api_id': 8471769,
                            'player_1_eh_id': 'NIKLAS.HJALMARSSON',},
                    61: {'period_seconds': 666, 'period_time': '11:06', 'game_seconds': 666,},
                    78: {'period_seconds': 902, 'period_time': '15:02', 'game_seconds': 902,},
                    173: {'period_seconds': 850, 'period_time': '14:10', 'game_seconds': 2050,},

                            }

        bad_events = {x['event_idx']: x for x in api_events if x['event_idx'] in bad_idxs.keys()}

        for idx, new_values in bad_idxs.items():

            event = bad_events.get(idx)

            if event is not None:

                event.update(new_values)

    if game_id == 2019020840:

        bad_idxs = {313: {'period_seconds': 1112, 'period_time': '18:32', 'game_seconds': 3512},

                            }

        bad_events = {x['event_idx']: x for x in api_events if x['event_idx'] in bad_idxs.keys()}

        for idx, new_values in bad_idxs.items():

            event = bad_events.get(idx)

            if event is not None:

                event.update(new_values)

    if game_id == 2019020876:

        bad_idxs = {315: {'period_seconds': 1088, 'period_time': '18:08', 'game_seconds': 3488},
                    316: {'period_seconds': 1093, 'period_time': '18:13', 'game_seconds': 3493},

                            }

        bad_events = {x['event_idx']: x for x in api_events if x['event_idx'] in bad_idxs.keys()}

        for idx, new_values in bad_idxs.items():

            event = bad_events.get(idx)

            if event is not None:

                event.update(new_values)

    if game_id == 2020020039:

        bad_idxs = {91: {'period_seconds': 46, 'period_time': '0:46', 'game_seconds': 1246,},
                    93: {'period_seconds': 48, 'period_time': '0:48', 'game_seconds': 1248,
                            'player_1': 'TOMAS HERTL', 'player_1_api_id': 8476881,
                            'player_1_eh_id': 'TOMAS.HERTL', 'player_2': "RYAN O'REILLY",
                            'player_2_api_id': 8475158, 'player_2_eh_id': "RYAN.O'REILLY"},
                    97: {'period_seconds': 134, 'period_time': '2:14', 'game_seconds': 1334},}

        bad_events = {x['event_idx']: x for x in api_events if x['event_idx'] in bad_idxs.keys()}

        for idx, new_values in bad_idxs.items():

            event = bad_events.get(idx)

            if event is not None:

                event.update(new_values)

    if game_id == 2020020407:

        misses = {x['game_seconds']: x for x in api_events if x['event'] == 'MISS'}

        bad_misses = {511: {'player_1': 'MIKHAIL SERGACHEV', 'player_1_eh_id': 'MIKHAIL.SERGACHEV', 'player_1_api_id': 8479410},}

        for game_seconds, new_values in bad_misses.items():

            miss = misses.get(game_seconds)

            if miss is not None:

                miss.update(new_values)

    if game_id == 2020020408:

        bad_faceoffs = [740, 2400, 2734, 2834]

        bad_faceoffs = [x for x in api_events if (x['game_seconds'] in bad_faceoffs and x['event'] == 'FAC')]

        for bad_faceoff in bad_faceoffs:

            if bad_faceoff['event_team'] == 'TOR':

                event_team = 'WPG'

                event_team_name = 'WINNIPEG JETS'

            elif bad_faceoff['event_team'] == 'WPG':

                event_team = 'TOR'

                event_team_name = 'TORONTO MAPLE LEAFS'

            new_values = {'event_team': event_team,
                            'event_team_name': event_team_name,
                            'player_1': bad_faceoff['player_2'],
                            'player_1_api_id': bad_faceoff['player_2_api_id'],
                            'player_1_eh_id': bad_faceoff['player_2_eh_id'],
                            'player_2': bad_faceoff['player_1'],
                            'player_2_api_id': bad_faceoff['player_1_api_id'],
                            'player_2_eh_id': bad_faceoff['player_1_eh_id'],}

            bad_faceoff.update(new_values)

    if game_id == 2020020456:

        blocks = {x['game_seconds']: x for x in api_events if x['event'] == 'BLOCK'}

        bad_blocks = {2269: {'period_seconds': 1068, 'period_time': '17:48', 'game_seconds': 2268},}

        for game_seconds, new_values in bad_blocks.items():

            block = blocks.get(game_seconds)

            if block is not None:

                block.update(new_values)

    if game_id == 2020020860:

        shots = {x['game_seconds']: x for x in api_events if x['event'] == 'SHOT'}

        bad_shots = {3869: {'period_seconds': 270, 'period_time': '4:30', 'game_seconds': 3870},}

        for game_seconds, new_values in bad_shots.items():

            shot = shots.get(game_seconds)

            if shot is not None:

                shot.update(new_values)

    if game_id == 2021020039:

        hits = {x['game_seconds']: x for x in api_events if x['event'] == 'HIT'}

        bad_hits = {3381: {'period_seconds': 980, 'period_time': '16:20', 'game_seconds': 3380},}

        for game_seconds, new_values in bad_hits.items():

            hit = hits.get(game_seconds)

            if hit is not None:

                hit.update(new_values)

    if game_id == 2021020041:

        hits = {x['game_seconds']: x for x in api_events if x['event'] == 'HIT'}

        bad_hits = {3486: {'period_seconds': 1085, 'period_time': '18:05', 'game_seconds': 3485},}

        for game_seconds, new_values in bad_hits.items():

            hit = hits.get(game_seconds)

            if hit is not None:

                hit.update(new_values)

    if game_id == 2021020086:

        bad_faceoffs = [1725, 2044, 3316, 3420]

        bad_faceoffs = [x for x in api_events if (x['game_seconds'] in bad_faceoffs and x['event'] == 'FAC')]

        for bad_faceoff in bad_faceoffs:

            if bad_faceoff['event_team'] == 'NYR':

                event_team = 'CGY'

                event_team_name = 'CALGARY FLAMES'

            elif bad_faceoff['event_team'] == 'CGY':

                event_team = 'NYR'

                event_team_name = 'NEW YORK RANGERS'

            new_values = {'event_team': event_team,
                            'event_team_name': event_team_name,
                            'player_1': bad_faceoff['player_2'],
                            'player_1_api_id': bad_faceoff['player_2_api_id'],
                            'player_1_eh_id': bad_faceoff['player_2_eh_id'],
                            'player_2': bad_faceoff['player_1'],
                            'player_2_api_id': bad_faceoff['player_1_api_id'],
                            'player_2_eh_id': bad_faceoff['player_1_eh_id'],}

            bad_faceoff.update(new_values)

    if game_id == 2021020112:

        bad_faceoffs = [0, 965, 1137, 1268, 1844]

        bad_faceoffs = [x for x in api_events if (x['game_seconds'] in bad_faceoffs and x['event'] == 'FAC')]

        for bad_faceoff in bad_faceoffs:

            if bad_faceoff['event_team'] == 'NYR':

                event_team = 'CBJ'

                event_team_name = 'COLUMBUS BLUE JACKETS'

            elif bad_faceoff['event_team'] == 'CBJ':

                event_team = 'NYR'

                event_team_name = 'NEW YORK RANGERS'

            new_values = {'event_team': event_team,
                            'event_team_name': event_team_name,
                            'player_1': bad_faceoff['player_2'],
                            'player_1_api_id': bad_faceoff['player_2_api_id'],
                            'player_1_eh_id': bad_faceoff['player_2_eh_id'],
                            'player_2': bad_faceoff['player_1'],
                            'player_2_api_id': bad_faceoff['player_1_api_id'],
                            'player_2_eh_id': bad_faceoff['player_1_eh_id'],}

            bad_faceoff.update(new_values)

    if game_id == 2021020224:

        bad_faceoffs = [2534]

        bad_faceoffs = [x for x in api_events if (x['game_seconds'] in bad_faceoffs and x['event'] == 'FAC')]

        for bad_faceoff in bad_faceoffs:

            new_values = {'player_1': 'ALEX BELZILE',
                            'player_1_api_id': 8475968,
                            'player_1_eh_id': 'ALEX.BELZILE',
                            'player_2': 'TOMAS NOSEK',
                            'player_2_api_id': 8477931,
                            'player_2_eh_id': 'TOMAS.NOSEK',}

            bad_faceoff.update(new_values)

    if game_id == 2021020347:

        shots = {x['game_seconds']: x for x in api_events if x['event'] == 'SHOT'}

        bad_shots = {2726: {'period_seconds': 321, 'period_time': '5:21', 'game_seconds': 2721},}

        for game_seconds, new_values in bad_shots.items():

            shot = shots.get(game_seconds)

            if shot is not None:

                shot.update(new_values)

    if game_id == 2021020537:

        hits = {x['game_seconds']: x for x in api_events if x['event'] == 'HIT'}

        bad_hits = {3627: {'period_seconds': 28, 'period_time': '0:28', 'game_seconds': 3628},}

        for game_seconds, new_values in bad_hits.items():

            hit = hits.get(game_seconds)

            if hit is not None:

                hit.update(new_values)

    if game_id == 2021020566:

        ## Some events can't be reconciled, this is the best we get

        bad_idxs = {6: {'period_seconds': 56, 'period_time': '0:56', 'game_seconds': 56,},
                    7: {'period_seconds': 98, 'period_time': '1:38', 'game_seconds': 98,},
                    8: {'period_seconds': 115, 'period_time': '1:55', 'game_seconds': 115,},
                    12: {'period_seconds': 158, 'period_time': '2:38', 'game_seconds': 158,},
                    15: {'period_seconds': 175, 'period_time': '2:55', 'game_seconds': 175,},
                    16: {'period_seconds': 176, 'period_time': '2:56', 'game_seconds': 176,},
                    17: {'period_seconds': 177, 'period_time': '2:57', 'game_seconds': 177,},
                    18: {'period_seconds': 210, 'period_time': '3:10', 'game_seconds': 210,},
                    19: {'period_seconds': 228, 'period_time': '3:28', 'game_seconds': 228,},
                    21: {'period_seconds': 255, 'period_time': '4:15', 'game_seconds': 255,},
                    24: {'period_seconds': 291, 'period_time': '4:51', 'game_seconds': 291,},
                    25: {'period_seconds': 306, 'period_time': '5:06', 'game_seconds': 306,},
                    26: {'period_seconds': 308, 'period_time': '5:08', 'game_seconds': 308,},
                    27: {'period_seconds': 343, 'period_time': '5:43', 'game_seconds': 343,},
                    28: {'period_seconds': 361, 'period_time': '6:01', 'game_seconds': 361,},
                    29: {'period_seconds': 347, 'period_time': '5:47', 'game_seconds': 347,},
                    31: {'period_seconds': 300, 'period_time': '5:00', 'game_seconds': 301},
                    32: {'period_seconds': 369, 'period_time': '6:09', 'game_seconds': 369},
                    33: {'period_seconds': 380, 'period_time': '6:20', 'game_seconds': 380},
                    39: {'period_seconds': 428, 'period_time': '7:08', 'game_seconds': 428},
                    40: {'period_seconds': 445, 'period_time': '7:25', 'game_seconds': 445},
                    41: {'period_seconds': 428, 'period_time': '7:08', 'game_seconds': 428},
                    43: {'period_seconds': 467, 'period_time': '7:47', 'game_seconds': 467},
                    45: {'period_seconds': 510, 'period_time': '8:30', 'game_seconds': 510},
                    46: {'period_seconds': 552, 'period_time': '9:12', 'game_seconds': 552},
                    47: {'period_seconds': 601, 'period_time': '10:01', 'game_seconds': 601},
                    51: {'period_seconds': 627, 'period_time': '10:27', 'game_seconds': 627},
                    52: {'period_seconds': 659, 'period_time': '10:59', 'game_seconds': 659,
                        'player_1': 'JEFF SKINNER', 'player_1_api_id': 8475784,
                        'player_1_eh_id': 'JEFF.SKINNER',},
                    53: {'period_seconds': 680, 'period_time': '11:20', 'game_seconds': 680},
                    55: {'period_seconds': 705, 'period_time': '11:45', 'game_seconds': 705},
                    57: {'period_seconds': 727, 'period_time': '12:07', 'game_seconds': 727},
                    58: {'period_seconds': 741, 'period_time': '12:21', 'game_seconds': 741},
                    59: {'period_seconds': 751, 'period_time': '12:31', 'game_seconds': 751},
                    63: {'period_seconds': 801, 'period_time': '13:21', 'game_seconds': 801},
                    67: {'period_seconds': 842, 'period_time': '14:02', 'game_seconds': 842,
                        'player_2': 'JEFF SKINNER', 'player_2_api_id': 8475784,
                        'player_2_eh_id': 'JEFF.SKINNER',},
                    68: {'period_seconds': 853, 'period_time': '14:13', 'game_seconds': 853},
                    73: {'player_1': 'VICTOR OLOFSSON', 'player_1_api_id': 8478109, 'player_1_eh_id': 'VICTOR.OLOFSSON',},
                    74: {'period_seconds': 921, 'period_time': '15:21', 'game_seconds': 921},
                    81: {'period_seconds': 995, 'period_time': '16:35', 'game_seconds': 995},
                    84: {'period_seconds': 993, 'period_time': '16:33', 'game_seconds': 993,
                        'player_1': 'CODY EAKIN', 'player_1_api_id': 8475236,
                        'player_1_eh_id': 'CODY.EAKIN',},
                    85: {'period_seconds': 1020, 'period_time': '17:00', 'game_seconds': 1020},
                    88: {'period_seconds': 1029, 'period_time': '17:09', 'game_seconds': 1029},
                    89: {'period_seconds': 1058, 'period_time': '17:38', 'game_seconds': 1058,
                        'player_2': 'CONNOR CLIFTON', 'player_2_api_id': 8477365,
                        'player_2_eh_id': 'CONNOR.CLIFTON',},
                    92: {'period_seconds': 1080, 'period_time': '18:00', 'game_seconds': 1080},
                    93: {'period_seconds': 1093, 'period_time': '18:13', 'game_seconds': 1093},
                    96: {'period_seconds': 1115, 'period_time': '18:35', 'game_seconds': 1115},
                    99: {'period_seconds': 1119, 'period_time': '18:39', 'game_seconds': 1119},
                    100: {'period_seconds': 1122, 'period_time': '18:42', 'game_seconds': 1122},
                    103: {'period_seconds': 1175, 'period_time': '19:35', 'game_seconds': 1175},
                    115: {'period_seconds': 41, 'period_time': '0:41', 'game_seconds': 1241},
                    116: {'period_seconds': 42, 'period_time': '0:42', 'game_seconds': 1242},
                    117: {'period_seconds': 73, 'period_time': '1:13', 'game_seconds': 1273},
                    118: {'period_seconds': 84, 'period_time': '1:24', 'game_seconds': 1284},
                    121: {'period_seconds': 126, 'period_time': '2:06', 'game_seconds': 1326},
                    122: {'period_seconds': 125, 'period_time': '2:05', 'game_seconds': 1325},
                    123: {'period_seconds': 155, 'period_time': '2:35', 'game_seconds': 1355},
                    124: {'period_seconds': 173, 'period_time': '2:53', 'game_seconds': 1373},
                    127: {'period_seconds': 186, 'period_time': '3:06', 'game_seconds': 1386},
                    128: {'period_seconds': 197, 'period_time': '3:06', 'game_seconds': 1397},
                    129: {'period_seconds': 204, 'period_time': '3:24', 'game_seconds': 1404},
                    130: {'period_seconds': 219, 'period_time': '3:39', 'game_seconds': 1419},
                    133: {'period_seconds': 225, 'period_time': '3:45', 'game_seconds': 1425},
                    141: {'period_seconds': 365, 'period_time': '6:05', 'game_seconds': 1565},
                    142: {'period_seconds': 384, 'period_time': '6:24', 'game_seconds': 1584},
                    143: {'period_seconds': 397, 'period_time': '6:37', 'game_seconds': 1597},
                    144: {'period_seconds': 405, 'period_time': '6:45', 'game_seconds': 1605},
                    145: {'period_seconds': 413, 'period_time': '6:53', 'game_seconds': 1613},
                    146: {'period_seconds': 427, 'period_time': '7:07', 'game_seconds': 1627},
                    147: {'period_seconds': 488, 'period_time': '8:08', 'game_seconds': 1688},
                    150: {'period_seconds': 546, 'period_time': '9:06', 'game_seconds': 1746},
                    151: {'period_seconds': 597, 'period_time': '9:57', 'game_seconds': 1797},
                    157: {'period_seconds': 662, 'period_time': '11:02', 'game_seconds': 1862},
                    160: {'period_seconds': 676, 'period_time': '11:16', 'game_seconds': 1876},
                    163: {'period_seconds': 732, 'period_time': '12:12', 'game_seconds': 1932},
                    164: {'period_seconds': 758, 'period_time': '12:38', 'game_seconds': 1958},
                    165: {'period_seconds': 780, 'period_time': '13:00', 'game_seconds': 1980},
                    166: {'period_seconds': 788, 'period_time': '13:08', 'game_seconds': 1988},
                    169: {'period_seconds': 796, 'period_time': '13:16', 'game_seconds': 1996},
                    170: {'period_seconds': 805, 'period_time': '13:25', 'game_seconds': 2005},
                    171: {'period_seconds': 807, 'period_time': '13:27', 'game_seconds': 2007},
                    176: {'period_seconds': 847, 'period_time': '14:07', 'game_seconds': 2047},
                    180: {'period_seconds': 869, 'period_time': '14:29', 'game_seconds': 2069},
                    181: {'period_seconds': 896, 'period_time': '14:56', 'game_seconds': 2096},
                    182: {'period_seconds': 917, 'period_time': '15:17', 'game_seconds': 2117},
                    183: {'period_seconds': 949, 'period_time': '15:49', 'game_seconds': 2149},
                    184: {'period_seconds': 962, 'period_time': '16:02', 'game_seconds': 2162},
                    187: {'period_seconds': 972, 'period_time': '16:12', 'game_seconds': 2172},
                    188: {'period_seconds': 986, 'period_time': '16:26', 'game_seconds': 2186},
                    189: {'period_seconds': 995, 'period_time': '16:35', 'game_seconds': 2195},
                    190: {'period_seconds': 1063, 'period_time': '17:43', 'game_seconds': 2263},
                    191: {'period_seconds': 1091, 'period_time': '18:11', 'game_seconds': 2291},
                    192: {'period_seconds': 1092, 'period_time': '18:12', 'game_seconds': 2292},
                    195: {'period_seconds': 1116, 'period_time': '18:36', 'game_seconds': 2316},
                    198: {'period_seconds': 1180, 'period_time': '18:12', 'game_seconds': 2380},
                    206: {'period_seconds': 44, 'period_time': '0:44', 'game_seconds': 2444},
                    207: {'period_seconds': 53, 'period_time': '0:53', 'game_seconds': 2453},
                    208: {'period_seconds': 58, 'period_time': '0:58', 'game_seconds': 2458},
                    209: {'period_seconds': 62, 'period_time': '1:02', 'game_seconds': 2462},
                    210: {'period_seconds': 79, 'period_time': '1:19', 'game_seconds': 2479},
                    211: {'period_seconds': 81, 'period_time': '1:21', 'game_seconds': 2481},
                    212: {'period_seconds': 88, 'period_time': '1:28', 'game_seconds': 2488},
                    215: {'period_seconds': 102, 'period_time': '1:42', 'game_seconds': 2502},
                    218: {'period_seconds': 130, 'period_time': '2:10', 'game_seconds': 2530},
                    219: {'period_seconds': 138, 'period_time': '2:18', 'game_seconds': 2538},
                    222: {'period_seconds': 176, 'period_time': '2:56', 'game_seconds': 2576},
                    223: {'period_seconds': 184, 'period_time': '3:04', 'game_seconds': 2584},
                    226: {'period_seconds': 202, 'period_time': '3:22', 'game_seconds': 2602},
                    239: {'period_seconds': 302, 'period_time': '5:02', 'game_seconds': 2702},
                    240: {'period_seconds': 342, 'period_time': '5:42', 'game_seconds': 2742},
                    241: {'period_seconds': 349, 'period_time': '5:49', 'game_seconds': 2749},
                    242: {'period_seconds': 372, 'period_time': '6:12', 'game_seconds': 2772},
                    249: {'period_seconds': 397, 'period_time': '6:37', 'game_seconds': 2797},
                    254: {'period_seconds': 455, 'period_time': '7:35', 'game_seconds': 2855},
                    255: {'period_seconds': 459, 'period_time': '7:39', 'game_seconds': 2859},
                    258: {'period_seconds': 475, 'period_time': '7:55', 'game_seconds': 2875},
                    261: {'period_seconds': 483, 'period_time': '8:03', 'game_seconds': 2883},
                    263: {'period_seconds': 499, 'period_time': '8:19', 'game_seconds': 2899},
                    267: {'period_seconds': 518, 'period_time': '8:38', 'game_seconds': 2918},
                    266: {'period_seconds': 523, 'period_time': '8:43', 'game_seconds': 2923},
                    270: {'period_seconds': 560, 'period_time': '9:20', 'game_seconds': 2960},
                    271: {'period_seconds': 567, 'period_time': '9:27', 'game_seconds': 2967},
                    272: {'period_seconds': 622, 'period_time': '10:22', 'game_seconds': 3022},
                    273: {'period_seconds': 630, 'period_time': '10:30', 'game_seconds': 3030},
                    276: {'period_seconds': 656, 'period_time': '10:56', 'game_seconds': 3056},
                    278: {'period_seconds': 689, 'period_time': '11:29', 'game_seconds': 3089},
                    279: {'period_seconds': 699, 'period_time': '11:39', 'game_seconds': 3099},
                    280: {'period_seconds': 701, 'period_time': '11:41', 'game_seconds': 3101},
                    281: {'period_seconds': 732, 'period_time': '12:12', 'game_seconds': 3132},
                    286: {'period_seconds': 845, 'period_time': '14:05', 'game_seconds': 3245},
                    287: {'period_seconds': 861, 'period_time': '14:21', 'game_seconds': 3261},
                    290: {'period_seconds': 867, 'period_time': '14:27', 'game_seconds': 3267},
                    291: {'period_seconds': 880, 'period_time': '14:40', 'game_seconds': 3280},
                    292: {'period_seconds': 908, 'period_time': '15:08', 'game_seconds': 3308},
                    293: {'period_seconds': 922, 'period_time': '15:22', 'game_seconds': 3322},
                    294: {'period_seconds': 931, 'period_time': '15:31', 'game_seconds': 3331},
                    295: {'period_seconds': 944, 'period_time': '15:44', 'game_seconds': 3344},
                    297: {'period_seconds': 978, 'period_time': '16:18', 'game_seconds': 3378},
                    298: {'period_seconds': 991, 'period_time': '16:31', 'game_seconds': 3391},
                    301: {'period_seconds': 1001, 'period_time': '16:41', 'game_seconds': 3401},
                    302: {'period_seconds': 1023, 'period_time': '17:03', 'game_seconds': 3423},
                    303: {'period_seconds': 1089, 'period_time': '18:09', 'game_seconds': 3489},
                    304: {'period_seconds': 1106, 'period_time': '18:26', 'game_seconds': 3506},
                    309: {'period_seconds': 1143, 'period_time': '19:03', 'game_seconds': 3543},
                    310: {'period_seconds': 1157, 'period_time': '19:17', 'game_seconds': 3557},
                    311: {'period_seconds': 1193, 'period_time': '15:53', 'game_seconds': 3593},
                    317: {'period_seconds': 17, 'period_time': '0:17', 'game_seconds': 3617},


        }

        bad_events = {x['event_idx']: x for x in api_events if x['event_idx'] in bad_idxs.keys()}

        for idx, new_values in bad_idxs.items():

            event = bad_events.get(idx)

            if event is not None:

                event.update(new_values)

    if game_id == 2021020682:

        faceoffs = {x['game_seconds']: x for x in api_events if x['event'] == 'FAC'}

        bad_faceoffs = {1695: {'period_seconds': 492, 'period_time': '8:12', 'game_seconds': 1692},} 

        for game_seconds, new_values in bad_faceoffs.items():

            faceoff = faceoffs.get(game_seconds)

            if faceoff is not None:

                faceoff.update(new_values)

        challenges = {x['game_seconds']: x for x in api_events if x['event'] == 'CHL'}

        bad_challenges = {1695: {'period_seconds': 492, 'period_time': '8:12', 'game_seconds': 1692},} 

        for game_seconds, new_values in bad_challenges.items():

            challenge = challenges.get(game_seconds)

            if challenge is not None:

                challenge.update(new_values)

    if game_id == 2021020752:

        bad_shots = [x for x in api_events if x['event_idx'] == 385]

        new_values = {'player_1': 'RICKARD RAKELL',
                        'player_1_api_id': 8476483,
                        'player_1_eh_id': 'RICKARD.RAKELL',
            }

        bad_shots[0].update(new_values)

    if game_id == 2021020795:

        blocks = {x['game_seconds']: x for x in api_events if x['event'] == 'BLOCK'}

        bad_blocks = {3028: {'period_seconds': 626, 'period_time': '10:26', 'game_seconds': 3026},}

        for game_seconds, new_values in bad_blocks.items():

            block = blocks.get(game_seconds)

            if block is not None:

                block.update(new_values)

    if game_id == 2021020874:

        blocks = {x['game_seconds']: x for x in api_events if x['event'] == 'BLOCK'}

        bad_blocks = {1013: {'period_seconds': 1012, 'period_time': '16:52', 'game_seconds': 1012},}

        for game_seconds, new_values in bad_blocks.items():

            block = blocks.get(game_seconds)

            if block is not None:

                block.update(new_values)

    if game_id == 2021020894:

        misses = {x['game_seconds']: x for x in api_events if x['event'] == 'MISS'}

        bad_misses = {3501: {'period_seconds': 1107, 'period_time': '18:27', 'game_seconds': 3507},}

        for game_seconds, new_values in bad_misses.items():

            miss = misses.get(game_seconds)

            if miss is not None:

                miss.update(new_values)

    if game_id == 2021020997:

        gives = {x['game_seconds']: x for x in api_events if x['event'] == 'GIVE'}

        bad_gives = {1418: {'period_seconds': 194, 'period_time': '3:14', 'game_seconds': 1394},}

        for game_seconds, new_values in bad_gives.items():

            give = gives.get(game_seconds)

            if give is not None:

                give.update(new_values)

    if game_id == 2021021008:

        bad_faceoffs = [522, 940, 2155]

        bad_faceoffs = [x for x in api_events if (x['game_seconds'] in bad_faceoffs and x['event'] == 'FAC')]

        for bad_faceoff in bad_faceoffs:

            if bad_faceoff['event_team'] == 'STL':

                event_team = 'WSH'

                event_team_name = 'WASHINGTON CAPITALS'

            elif bad_faceoff['event_team'] == 'WSH':

                event_team = 'STL'

                event_team_name = 'ST. LOUIS BLUES'

            new_values = {'event_team': event_team,
                            'event_team_name': event_team_name,
                            'player_1': bad_faceoff['player_2'],
                            'player_1_api_id': bad_faceoff['player_2_api_id'],
                            'player_1_eh_id': bad_faceoff['player_2_eh_id'],
                            'player_2': bad_faceoff['player_1'],
                            'player_2_api_id': bad_faceoff['player_1_api_id'],
                            'player_2_eh_id': bad_faceoff['player_1_eh_id'],}

            bad_faceoff.update(new_values)

    if game_id == 2021021039:

        shots = {x['game_seconds']: x for x in api_events if x['event'] == 'SHOT'}

        bad_shots = {58: {'period_seconds': 57, 'period_time': '0:57', 'game_seconds': 57},}

        for game_seconds, new_values in bad_shots.items():

            shot = shots.get(game_seconds)

            if shot is not None:

                shot.update(new_values)

    return api_events