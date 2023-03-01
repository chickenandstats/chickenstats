import pandas as pd
import numpy as np

from tqdm.auto import tqdm


def prep_box_score(
    pbp,
    level: str = "game",
    strengths: bool = True,
    score: bool = False,
    teammates: bool = False,
    forwards: bool = False,
    defense: bool = False,
    own_goalie: bool = False,
    opposition: bool = False,
    opp_forwards: bool = False,
    opp_defense: bool = False,
    opp_goalie: bool = False,
):

    df = pbp.copy()

    if level == "season":

        group_list = ["season", "team"]

    if level == "session":

        group_list = ["season", "session", "team"]

    if level == "game":

        group_list = ["season", "game_id", "game_date", "session", "team", "opp_team"]

    if level == "period":

        group_list = [
            "season",
            "game_id",
            "game_date",
            "session",
            "team",
            "opp_team",
            "period",
        ]

    if strengths == True:

        group_list.append("strength_state")

    if score == True:

        group_list.append("score_state")

        group_list.append("score_diff")

    opp_forwards_cols = [
        "opp_forwards_id",
        "opp_forwards",
        "opp_forwards_hands",
        "opp_forwards_ages",
        "opp_forwards_ages_mean",
        "opp_forwards_height",
        "opp_forwards_height_mean",
        "opp_forwards_weight",
        "opp_forwards_weight_mean",
    ]

    opp_defense_cols = [
        "opp_defense_id",
        "opp_defense",
        "opp_defense_hands",
        "opp_defense_ages",
        "opp_defense_ages_mean",
        "opp_defense_height",
        "opp_defense_height_mean",
        "opp_defense_weight",
        "opp_defense_weight_mean",
    ]

    opp_goalie_cols = [
        "opp_goalie_id",
        "opp_goalie",
        "opp_goalie_catches",
        "opp_goalie_age",
        "opp_goalie_height",
        "opp_goalie_weight",
    ]

    opp_skater_cols = [
        "opp_team_on_id",
        "opp_team_on",
        "opp_team_on_positions",
        "opp_team_on_hands",
        "opp_team_on_ages",
        "opp_team_on_ages_mean",
        "opp_team_on_height",
        "opp_team_on_height_mean",
        "opp_team_on_weight",
        "opp_team_on_weight_mean",
    ]

    if opposition == True:

        group_list.extend(opp_skater_cols)

        if opp_goalie != True:

            group_list.extend(opp_goalie_cols)

    if opp_forwards == True:

        group_list.extend(opp_forwards_cols)

    if opp_defense == True:

        group_list.extend(opp_defense_cols)

    if opp_goalie == True:

        group_list.extend(opp_goalie_cols)

    team_forwards_cols = [
        "forwards_id",
        "forwards",
        "forwards_hands",
        "forwards_ages",
        "forwards_ages_mean",
        "forwards_height",
        "forwards_height_mean",
        "forwards_weight",
        "forwards_weight_mean",
    ]

    team_defense_cols = [
        "defense_id",
        "defense",
        "defense_hands",
        "defense_ages",
        "defense_ages_mean",
        "defense_height",
        "defense_height_mean",
        "defense_weight",
        "defense_weight_mean",
    ]

    team_goalie_cols = [
        "own_goalie_id",
        "own_goalie",
        "own_goalie_catches",
        "own_goalie_age",
        "own_goalie_height",
        "own_goalie_weight",
    ]

    team_skater_cols = [
        "teammates_id",
        "teammates",
        "teammates_positions",
        "teammates_hands",
        "teammates_ages",
        "teammates_ages_mean",
        "teammates_height",
        "teammates_height_mean",
        "teammates_weight",
        "teammates_weight_mean",
    ]

    if teammates == True:

        group_list.extend(team_skater_cols)

        if own_goalie == False:

            group_list.extend(team_goalie_cols)

    if forwards == True:

        group_list.extend(team_forwards_cols)

    if defense == True:

        group_list.extend(team_defense_cols)

    if own_goalie == True:

        group_list.extend(team_goalie_cols)

    if (
        "opp_goalie" or "opp_defense" or "opp_forwards" in group_list
    ) and "opp_team" not in group_list:

        group_list.append("opp_team")

    merge_columns = group_list + [
        "player",
        "eh_id",
        "api_id",
        "age",
        "position",
        "hand",
        "height",
        "weight",
    ]

    box_score = pd.DataFrame(columns=merge_columns)

    for num in range(1, 4):

        player = f"player_{num}"

        eh_id = f"player_{num}_eh_id"

        api_id = f"player_{num}_api_id"

        age = f"player_{num}_age"

        position = f"player_{num}_position"

        hand = f"player_{num}_hand"

        height = f"player_{num}_height"

        weight = f"player_{num}_weight"

        player_cols = [player, eh_id, api_id, position, age, hand, height, weight]

        if level == "season":

            group_base = ["season", "event_team"] + player_cols

        if level == "session":

            group_base = ["season", "session", "event_team"] + player_cols

        if level == "game":

            group_base = [
                "season",
                "game_id",
                "game_date",
                "session",
                "event_team",
                "opp_team",
            ] + player_cols

        if level == "period":

            group_base = [
                "season",
                "game_id",
                "game_date",
                "session",
                "event_team",
                "opp_team",
                "period",
            ] + player_cols

        if num == 1:

            strength_group = []
            score_group = []
            opposition_group = []
            teammates_group = []

            if strengths == True:

                strength_group.append("strength_state")

            if score == True:

                score_group.extend(["score_state", "score_diff"])

            if opposition == True:

                opposition_group.extend(opp_skater_cols)

                if opp_goalie == False:

                    opposition_group.extend(opp_goalie_cols)

            if opp_forwards == True:

                opposition_group.extend(opp_forwards_cols)

            if opp_defense == True:

                opposition_group.extend(opp_defense_cols)

            if opp_goalie == True:

                opposition_group.extend(opp_goalie_cols)

            if teammates == True:

                teammates_group.extend(team_skater_cols)

                if own_goalie == False:

                    teammates_group.extend(team_goalie_cols)

            if forwards == True:

                teammates_group.extend(team_forwards_cols)

            if defense == True:

                teammates_group.extend(team_defense_cols)

            if own_goalie == True:

                teammates_group.extend(team_goalie_cols)

            group_list = group_base.copy()

            group_list = (
                group_list
                + strength_group
                + score_group
                + teammates_group
                + opposition_group
            )

            if (
                "opp_goalie" or "opp_defense" or "opp_forwards" in group_list
            ) and "opp_team" not in group_list:

                group_list.append("opp_team")

            agg_stats = [
                "block",
                "fac",
                "give",
                "goal",
                "hit",
                "miss",
                "penl",
                "pen0",
                "pen2",
                "pen4",
                "pen5",
                "pen10",
                "shot",
                "fenwick",
                "take",
                "ozf",
                "nzf",
                "dzf",
            ]

            agg_stats = {x: "sum" for x in agg_stats if x in df.columns}

            player_stats = df.groupby(group_list, as_index=False).agg(agg_stats)

            new_cols = {
                "block": "ibsf",
                "fac": "ifow",
                "give": "igive",
                "goal": "g",
                "hit": "ihf",
                "miss": "imsf",
                "penl": "ipent",
                "pen0": "ipent0",
                "pen2": "ipent2",
                "pen4": "ipent4",
                "pen5": "ipent5",
                "pen10": "ipent10",
                "shot": "isf",
                "fenwick": "iff",
                "take": "itake",
                "ozf": "iozfow",
                "nzf": "inzfow",
                "dzf": "idzfow",
                "event_team": "team",
            }

            col_names = [
                "player",
                "eh_id",
                "api_id",
                "position",
                "age",
                "hand",
                "height",
                "weight",
            ]

            new_cols.update(dict(zip(player_cols, col_names)))

            player_stats.rename(columns=new_cols, inplace=True)

            box_score = box_score.merge(player_stats, how="outer", on=merge_columns)

        if num == 2:

            player_types = pd.get_dummies(df[f"player_{num}_type"])

            required_cols = [
                "ASSIST",
                "DREWBY",
                "HITTEE",
                "LOSER",
                "SERVEDBY",
                "SHOOTER",
            ]

            for required_col in required_cols:

                if required_col not in player_types.columns:

                    player_types[required_col] = 0

            penl_cols = {
                "ipend": "penl",
                "ipend0": "pen0",
                "ipend2": "pen2",
                "ipend4": "pen4",
                "ipend5": "pen5",
                "ipend10": "pen10",
            }

            for new_name, old_name in penl_cols.items():

                if old_name in df.columns:

                    df[new_name] = df[old_name] - player_types.SERVEDBY

            strength_group1 = []
            strength_group2 = []

            score_group1 = []
            score_group2 = []

            opposition_group1 = []
            opposition_group2 = []

            teammates_group1 = []
            teammates_group2 = []

            if strengths == True:

                strength_group1.append("opp_strength_state")

                strength_group2.append("strength_state")

            if score == True:

                score_group1.extend(["opp_score_state", "opp_score_diff"])

                score_group2.extend(["score_state", "score_diff"])

            if opposition == True:

                opposition_group1.extend(team_skater_cols)

                opposition_group2.extend(opp_skater_cols)

                if opp_goalie == False:

                    opposition_group1.extend(team_goalie_cols)

                    opposition_group2.extend(opp_goalie_cols)

            if opp_forwards == True:

                opposition_group1.extend(team_forwards_cols)

                opposition_group2.extend(opp_forwards_cols)

            if opp_defense == True:

                opposition_group1.extend(team_defense_cols)

                opposition_group2.extend(opp_defense_cols)

            if opp_goalie == True:

                opposition_group1.extend(team_goalie_cols)

                opposition_group2.extend(opp_goalie_cols)

            if teammates == True:

                teammates_group1.extend(opp_skater_cols)

                teammates_group2.extend(team_skater_cols)

                if own_goalie == False:

                    teammates_group1.extend(opp_goalie_cols)

            if forwards == True:

                teammates_group1.extend(opp_forwards_cols)

                teammates_group2.extend(team_forwards_cols)

            if defense == True:

                teammates_group1.extend(opp_defense_cols)

                teammates_group2.extend(team_defense_cols)

            if own_goalie == True:

                teammates_group1.extend(opp_goalie_cols)

                teammates_group2.extend(team_goalie_cols)

            group_list1 = group_base.copy()

            group_list2 = group_base.copy()

            group_list1 = (
                group_list1
                + strength_group1
                + score_group1
                + teammates_group1
                + opposition_group1
            )

            group_list2 = (
                group_list2
                + strength_group2
                + score_group2
                + teammates_group2
                + opposition_group2
            )

            if level in ["season", "session"] and "opp_team" not in group_list1:

                group_list1 = [x.replace("event_team", "opp_team") for x in group_list1]

            if (
                "opp_defense" or "opp_goalie" or "opp_forwards" in group_list2
            ) and "opp_team" not in group_list2:

                group_list2.append("opp_team")

            if (
                "opp_defense" or "opp_forwards" or "opp_goalie" in group_list1
            ) and "event_team" not in group_list1:

                group_list1.append("event_team")

            agg_stats1 = [
                "block",
                "fac",
                "hit",
                "ipend",
                "ipend0",
                "ipend2",
                "ipend4",
                "ipend5",
                "ipend10",
                "ozf",
                "nzf",
                "dzf",
            ]

            agg_stats1 = {x: "sum" for x in agg_stats1 if x in df.columns}

            new_cols = {
                player: "player",
                eh_id: "eh_id",
                api_id: "api_id",
                age: "age",
                hand: "hand",
                position: "position",
                height: "height",
                weight: "weight",
                "event_team": "opp_team",
                "opp_team": "team",
                "opp_strength_state": "strength_state",
                "opp_score_state": "score_state",
                "opp_score_diff": "score_diff",
                "block": "ibsa",
                "fac": "ifol",
                "hit": "iht",
                "ozf": "iozfol",
                "nzf": "inzfol",
                "dzf": "idzfol",
                "opp_team_on_id": "teammates_id",
                "opp_team_on": "teammates",
                "opp_team_on_positions": "teammates_positions",
                "opp_team_on_hands": "teammates_hands",
                "opp_team_on_ages": "teammates_ages",
                "opp_team_on_ages_mean": "teammates_ages_mean",
                "opp_team_on_height": "teammates_height",
                "opp_team_on_height_mean": "teammates_height_mean",
                "opp_team_on_weight": "teammates_weight",
                "opp_team_on_weight_mean": "teammates_weight_mean",
                "opp_forwards": "forwards",
                "opp_forwards_id": "forwards_id",
                "opp_forwards_hands": "forwards_hands",
                "opp_forwards_ages": "forwards_ages",
                "opp_forwards_ages_mean": "forwards_ages_mean",
                "opp_forwards_height": "forwards_height",
                "opp_forwards_height_mean": "forwards_height_mean",
                "opp_forwards_weight": "forwards_weight",
                "opp_forwards_weight_mean": "forwards_weight_mean",
                "opp_defense": "defense",
                "opp_defense_id": "defense_id",
                "opp_defense_hands": "defense_hands",
                "opp_defense_ages": "defense_ages",
                "opp_defense_ages_mean": "defense_ages_mean",
                "opp_defense_height": "defense_height",
                "opp_defense_height_mean": "defense_height_mean",
                "opp_defense_weight": "defense_weight",
                "opp_defense_weight_mean": "defense_weight_mean",
                "opp_goalie": "own_goalie",
                "opp_goalie_id": "own_goalie_id",
                "opp_goalie_age": "own_goalie_age",
                "opp_goalie_catches": "own_goalie_catches",
                "opp_goalie_height": "own_goalie_height",
                "opp_goalie_weight": "own_goalie_weight",
                "teammates_id": "opp_team_on_id",
                "teammates": "opp_team_on",
                "teammates_positions": "opp_team_on_positions",
                "teammates_hands": "opp_team_on_hands",
                "teammates_ages": "opp_team_on_ages",
                "teammates_ages_mean": "opp_team_on_ages_mean",
                "teammates_height": "opp_team_on_height",
                "teammates_height_mean": "opp_team_on_height_mean",
                "teammates_weight": "opp_team_on_weight",
                "teammates_weight_mean": "opp_team_on_weight_mean",
                "forwards": "opp_forwards",
                "forwards_id": "opp_forwards_id",
                "forwards_hands": "opp_forwards_hands",
                "forwards_ages": "opp_forwards_ages",
                "forwards_ages_mean": "opp_forwards_ages_mean",
                "forwards_height": "opp_forwards_height",
                "forwards_height_mean": "opp_forwards_height_mean",
                "forwards_weight": "opp_forwards_weight",
                "forwards_weight_mean": "opp_forwards_weight_mean",
                "defense": "opp_defense",
                "defense_id": "opp_defense_id",
                "defense_hands": "opp_defense_hands",
                "defense_ages": "opp_defense_ages",
                "defense_ages_mean": "opp_defense_ages_mean",
                "defense_height": "opp_defense_height",
                "defense_height_mean": "opp_defense_height_mean",
                "defense_weight": "opp_defense_weight",
                "defense_weight_mean": "opp_defense_weight_mean",
                "own_goalie": "opp_goalie",
                "own_goalie_id": "opp_goalie_id",
                "own_goalie_age": "opp_goalie_age",
                "own_goalie_catches": "opp_goalie_catches",
                "own_goalie_height": "opp_goalie_height",
                "own_goalie_weight": "opp_goalie_weight",
            }

            events_we_want = ["HIT", "PENL", "FAC", "BLOCK"]

            opps = (
                df.loc[df.event.isin(events_we_want)]
                .groupby(group_list1, as_index=False)
                .agg(agg_stats1)
                .rename(columns=new_cols)
            )

            box_score = box_score.merge(opps, on=merge_columns, how="outer")

            agg_stats2 = ["goal"]

            agg_stats2 = {x: "sum" for x in agg_stats2 if x in df.columns}

            new_cols = {
                player: "player",
                eh_id: "eh_id",
                api_id: "api_id",
                age: "age",
                hand: "hand",
                position: "position",
                height: "height",
                weight: "weight",
                "goal": "a1",
                "event_team": "team",
            }

            events_we_want = ["GOAL"]

            own = (
                df.loc[df.event.isin(events_we_want)]
                .groupby(group_list2, as_index=False)
                .agg(agg_stats2)
                .rename(columns=new_cols)
            )

            box_score = box_score.merge(own, how="outer", on=merge_columns)

        if num == 3:

            group_list = (
                group_base
                + strength_group
                + score_group
                + teammates_group
                + opposition_group
            )

            agg_stats = ["goal"]

            agg_stats = {x: "sum" for x in agg_stats}

            new_cols = {
                player: "player",
                eh_id: "eh_id",
                api_id: "api_id",
                age: "age",
                hand: "hand",
                position: "position",
                height: "height",
                weight: "weight",
                "goal": "a2",
                "event_team": "team",
            }

            if (
                "opp_goalie" or "opp_defense" or "opp_forwards" in group_list
            ) and "opp_team" not in group_list:

                group_list.append("opp_team")

            events_we_want = ["GOAL"]

            player_df = (
                df.loc[df.event.isin(events_we_want)]
                .groupby(group_list, as_index=False)
                .agg(agg_stats)
                .rename(columns=new_cols)
            )

            box_score = box_score.merge(player_df, on=merge_columns, how="outer")

    box_score = box_score.fillna(0)

    icols = box_score.select_dtypes("float").columns

    box_score[icols] = box_score[icols].apply(pd.to_numeric, downcast="integer")

    columns = [
        "season",
        "session",
        "game_id",
        "game_date",
        "team",
        "opp_team",
        "player",
        "eh_id",
        "api_id",
        "position",
        "age",
        "height",
        "weight",
        "hand",
        "period",
        "strength_state",
        "score_state",
        "score_diff",
        "teammates",
        "forwards",
        "defense",
        "own_goalie",
        "opp_team_on",
        "opp_forwards",
        "opp_defense",
        "opp_goalie",
        "g",
        "a1",
        "a2",
        "isf",
        "imsf",
        "iff",
        "ibsf",
        "ibsa",
        "ihf",
        "iht",
        "igive",
        "itake",
        "ifow",
        "ifol",
        "iozfow",
        "iozfol",
        "inzfow",
        "inzfol",
        "idzfow",
        "idzfol",
        "ipent",
        "ipent0",
        "ipent2",
        "ipent4",
        "ipent5",
        "ipent10",
        "ipend",
        "ipend0",
        "ipend2",
        "ipend4",
        "ipend5",
        "ipend10",
        "teammates_id",
        "teammates_positions",
        "teammates_hands",
        "teammates_ages",
        "teammates_ages_mean",
        "teammates_height",
        "teammates_height_mean",
        "teammates_weight",
        "teammates_weight_mean",
        "forwards",
        "forwards_id",
        "forwards_hands",
        "forwards_ages",
        "forwards_ages_mean",
        "forwards_height",
        "forwards_height_mean",
        "forwards_weight",
        "forwards_weight_mean",
        "defense",
        "defense_id",
        "defense_hands",
        "defense_ages",
        "defense_ages_mean",
        "defense_height",
        "defense_height_mean",
        "defense_weight",
        "defense_weight_mean",
        "own_goalie_id",
        "own_goalie_catches",
        "own_goalie_age",
        "own_goalie_height",
        "own_goalie_weight",
        "opp_team_on_id",
        "opp_team_on_positions",
        "opp_team_on_hands",
        "opp_team_on_ages",
        "opp_team_on_ages_mean",
        "opp_team_on_height",
        "opp_team_on_height_mean",
        "opp_team_on_weight",
        "opp_team_on_weight_mean",
        "opp_forwards",
        "opp_forwards_id",
        "opp_forwards_hands",
        "opp_forwards_ages",
        "opp_forwards_ages_mean",
        "opp_forwards_height",
        "opp_forwards_height_mean",
        "opp_forwards_weight",
        "opp_forwards_weight_mean",
        "opp_defense",
        "opp_defense_id",
        "opp_defense_hands",
        "opp_defense_ages",
        "opp_defense_ages_mean",
        "opp_defense_height",
        "opp_defense_height_mean",
        "opp_defense_weight",
        "opp_defense_weight_mean",
        "opp_goalie_id",
        "opp_goalie_catches",
        "opp_goalie_age",
        "opp_goalie_height",
        "opp_goalie_weight",
    ]

    columns = [x for x in columns if x in box_score.columns]

    box_score = box_score[columns]

    return box_score


def prep_on_ice(
    pbp,
    level: str = "game",
    strengths: bool = True,
    score: bool = False,
    teammates: bool = False,
    forwards: bool = False,
    defense: bool = False,
    own_goalie: bool = False,
    opposition: bool = False,
    opp_forwards: bool = False,
    opp_defense: bool = False,
    opp_goalie: bool = False,
):

    df = pbp.copy()

    split_cols = [
        "teammates",
        "teammates_id",
        "teammates_ages",
        "teammates_positions",
        "teammates_hands",
        "teammates_height",
        "teammates_weight",
        "opp_team_on",
        "opp_team_on_id",
        "opp_team_on_ages",
        "opp_team_on_positions",
        "opp_team_on_hands",
        "opp_team_on_height",
        "opp_team_on_weight",
    ]

    for split_col in split_cols:

        split_df = df[split_col].str.split(", ", expand=True)

        new_cols = {x: f"{split_col}_{x + 1}" for x in split_df.columns}

        split_df.rename(columns=new_cols, inplace=True)

        df = df.merge(split_df, left_index=True, right_index=True, how="left")

    if level == "season":

        group_list = ["season", "team"]

    if level == "session":

        group_list = ["season", "session", "team"]

    if level == "game":

        group_list = ["season", "game_id", "game_date", "session", "team", "opp_team"]

    if level == "period":

        group_list = [
            "season",
            "game_id",
            "game_date",
            "session",
            "team",
            "opp_team",
            "period",
        ]

    if strengths == True:

        group_list.append("strength_state")

    if score == True:

        group_list.append("score_state")

        group_list.append("score_diff")

    opp_forwards_cols = [
        "opp_forwards_id",
        "opp_forwards",
        "opp_forwards_hands",
        "opp_forwards_ages",
        "opp_forwards_ages_mean",
        "opp_forwards_height",
        "opp_forwards_height_mean",
        "opp_forwards_weight",
        "opp_forwards_weight_mean",
    ]

    opp_defense_cols = [
        "opp_defense_id",
        "opp_defense",
        "opp_defense_hands",
        "opp_defense_ages",
        "opp_defense_ages_mean",
        "opp_defense_height",
        "opp_defense_height_mean",
        "opp_defense_weight",
        "opp_defense_weight_mean",
    ]

    opp_goalie_cols = [
        "opp_goalie_id",
        "opp_goalie",
        "opp_goalie_catches",
        "opp_goalie_age",
        "opp_goalie_height",
        "opp_goalie_weight",
    ]

    opp_skater_cols = [
        "opp_team_on_id",
        "opp_team_on",
        "opp_team_on_positions",
        "opp_team_on_hands",
        "opp_team_on_ages",
        "opp_team_on_ages_mean",
        "opp_team_on_height",
        "opp_team_on_height_mean",
        "opp_team_on_weight",
        "opp_team_on_weight_mean",
    ]

    if opposition == True:

        group_list.extend(opp_skater_cols)

        if opp_goalie != True:

            group_list.extend(opp_goalie_cols)

    if opp_forwards == True:

        group_list.extend(opp_forwards_cols)

    if opp_defense == True:

        group_list.extend(opp_defense_cols)

    if opp_goalie == True:

        group_list.extend(opp_goalie_cols)

    team_forwards_cols = [
        "forwards_id",
        "forwards",
        "forwards_hands",
        "forwards_ages",
        "forwards_ages_mean",
        "forwards_height",
        "forwards_height_mean",
        "forwards_weight",
        "forwards_weight_mean",
    ]

    team_defense_cols = [
        "defense_id",
        "defense",
        "defense_hands",
        "defense_ages",
        "defense_ages_mean",
        "defense_height",
        "defense_height_mean",
        "defense_weight",
        "defense_weight_mean",
    ]

    team_goalie_cols = [
        "own_goalie_id",
        "own_goalie",
        "own_goalie_catches",
        "own_goalie_age",
        "own_goalie_height",
        "own_goalie_weight",
    ]

    team_skater_cols = [
        "teammates_id",
        "teammates",
        "teammates_positions",
        "teammates_hands",
        "teammates_ages",
        "teammates_ages_mean",
        "teammates_height",
        "teammates_height_mean",
        "teammates_weight",
        "teammates_weight_mean",
    ]

    if teammates == True:

        group_list.extend(team_skater_cols)

        if own_goalie == False:

            group_list.extend(team_goalie_cols)

    if forwards == True:

        group_list.extend(team_forwards_cols)

    if defense == True:

        group_list.extend(team_defense_cols)

    if own_goalie == True:

        group_list.extend(team_goalie_cols)

    if (
        "opp_goalie" or "opp_defense" or "opp_forwards" in group_list
    ) and "opp_team" not in group_list:

        group_list.append("opp_team")

    merge_columns = group_list + [
        "player",
        "eh_id",
        "age",
        "position",
        "hand",
        "height",
        "weight",
    ]

    merge_columns_for = merge_columns

    event_list = []

    for n in range(1, 8):

        if f"teammates_id_{n}" not in df.columns:

            continue

        player_cols = [
            "teammates",
            "teammates_id",
            "teammates_ages",
            "teammates_positions",
            "teammates_hands",
            "teammates_height",
            "teammates_weight",
        ]

        player_cols = [f"{x}_{n}" for x in player_cols]

        player_grouping = []

        for col in group_list:

            if col == "team":

                player_grouping.append("event_team")

            else:

                player_grouping.append(col)

        player_grouping = player_grouping + player_cols

        agg_stats = [
            "block",
            "fac",
            "fenwick",
            "give",
            "goal",
            "hit",
            "miss",
            "penl",
            "pen0",
            "pen2",
            "pen5",
            "pen10",
            "shot",
            "take",
            "dzf",
            "nzf",
            "ozf",
            "event_length",
        ]

        agg_stats = {x: "sum" for x in agg_stats if x in df.columns}

        player_df = df.groupby(by=player_grouping, as_index=False, dropna=False).agg(
            agg_stats
        )

        new_cols = {
            "block": "BSF",
            "fac": "FOW",
            "fenwick": "FF",
            "give": "GIVE",
            "goal": "GF",
            "hit": "HF",
            "miss": "MSF",
            "penl": "PENT",
            "pen0": "PENT0",
            "pen2": "PENT2",
            "pen4": "PENT4",
            "pen5": "PENT5",
            "pen10": "PENT10",
            "shot": "SF",
            "take": "TAKE",
            "dzf": "DZFOW",
            "nzf": "NZFOW",
            "ozf": "OZFOW",
            "event_length": "toi",
            "event_team": "team",
        }

        player_cols = dict(
            zip(
                player_cols,
                ["player", "eh_id", "age", "position", "hand", "height", "weight"],
            )
        )

        new_cols.update(player_cols)

        player_df = player_df.rename(columns=new_cols)

        event_list.append(player_df)

    event_stats = pd.concat(event_list, ignore_index=True)

    agg_stats = {x: "sum" for x in event_stats.columns if x not in merge_columns}

    event_stats = event_stats.groupby(merge_columns, as_index=False, dropna=False).agg(
        agg_stats
    )

    if level == "season":

        group_list = ["season", "team"]

    if level == "session":

        group_list = ["season", "session", "team"]

    if level == "game":

        group_list = ["season", "game_id", "game_date", "session", "team", "opp_team"]

    if level == "period":

        group_list = [
            "season",
            "game_id",
            "game_date",
            "session",
            "team",
            "opp_team",
            "period",
        ]

    if strengths == True:

        group_list.append("opp_strength_state")

    if score == True:

        group_list.append("opp_score_state")

        group_list.append("opp_score_diff")

    team_forwards_cols = [
        "opp_forwards_id",
        "opp_forwards",
        "opp_forwards_hands",
        "opp_forwards_ages",
        "opp_forwards_ages_mean",
        "opp_forwards_height",
        "opp_forwards_height_mean",
        "opp_forwards_weight",
        "opp_forwards_weight_mean",
    ]

    team_defense_cols = [
        "opp_defense_id",
        "opp_defense",
        "opp_defense_hands",
        "opp_defense_ages",
        "opp_defense_ages_mean",
        "opp_defense_height",
        "opp_defense_height_mean",
        "opp_defense_weight",
        "opp_defense_weight_mean",
    ]

    team_goalie_cols = [
        "opp_goalie_id",
        "opp_goalie",
        "opp_goalie_catches",
        "opp_goalie_age",
        "opp_goalie_height",
        "opp_goalie_weight",
    ]

    team_skater_cols = [
        "opp_team_on_id",
        "opp_team_on",
        "opp_team_on_positions",
        "opp_team_on_hands",
        "opp_team_on_ages",
        "opp_team_on_ages_mean",
        "opp_team_on_height",
        "opp_team_on_height_mean",
        "opp_team_on_weight",
        "opp_team_on_weight_mean",
    ]

    if teammates == True:

        group_list.extend(team_skater_cols)

        if own_goalie == False:

            group_list.extend(team_goalie_cols)

    if forwards == True:

        group_list.extend(team_forwards_cols)

    if defense == True:

        group_list.extend(team_defense_cols)

    if own_goalie == True:

        group_list.extend(team_goalie_cols)

    opp_forwards_cols = [
        "forwards_id",
        "forwards",
        "forwards_hands",
        "forwards_ages",
        "forwards_ages_mean",
        "forwards_height",
        "forwards_height_mean",
        "forwards_weight",
        "forwards_weight_mean",
    ]

    opp_defense_cols = [
        "defense_id",
        "defense",
        "defense_hands",
        "defense_ages",
        "defense_ages_mean",
        "defense_height",
        "defense_height_mean",
        "defense_weight",
        "defense_weight_mean",
    ]

    opp_goalie_cols = [
        "own_goalie_id",
        "own_goalie",
        "own_goalie_catches",
        "own_goalie_age",
        "own_goalie_height",
        "own_goalie_weight",
    ]

    opp_skater_cols = [
        "teammates_id",
        "teammates",
        "teammates_positions",
        "teammates_hands",
        "teammates_ages",
        "teammates_ages_mean",
        "teammates_height",
        "teammates_height_mean",
        "teammates_weight",
        "teammates_weight_mean",
    ]

    if opposition == True:

        group_list.extend(opp_skater_cols)

        if opp_goalie == False:

            group_list.extend(opp_goalie_cols)

    if opp_forwards == True:

        group_list.extend(opp_forwards_cols)

    if opp_defense == True:

        group_list.extend(opp_defense_cols)

    if opp_goalie == True:

        group_list.extend(opp_goalie_cols)

    if (
        "own_goalie" or "defense" or "forwards" in group_list
    ) and "event_team" not in group_list:

        group_list.append("event_team")

    merge_columns = group_list + [
        "player",
        "eh_id",
        "age",
        "position",
        "hand",
        "height",
        "weight",
    ]

    opp_list = []

    for n in range(1, 8):

        if f"opp_team_on_id_{n}" not in df.columns:

            continue

        player_cols = [
            "opp_team_on",
            "opp_team_on_id",
            "opp_team_on_ages",
            "opp_team_on_positions",
            "opp_team_on_hands",
            "opp_team_on_height",
            "opp_team_on_weight",
        ]

        player_cols = [f"{x}_{n}" for x in player_cols]

        player_grouping = []

        for col in group_list:

            if col == "team" and "opp_team" not in group_list:

                player_grouping.append("opp_team")

            if col == "team" and "opp_team" in group_list:

                continue

            else:

                player_grouping.append(col)

        player_grouping = player_grouping + player_cols

        agg_stats = [
            "block",
            "fac",
            "fenwick",
            "give",
            "goal",
            "hit",
            "miss",
            "penl",
            "pen0",
            "pen2",
            "pen5",
            "pen10",
            "shot",
            "take",
            "dzf",
            "nzf",
            "ozf",
            "event_length",
        ]

        agg_stats = {x: "sum" for x in agg_stats if x in df.columns}

        player_df = df.groupby(by=player_grouping, as_index=False, dropna=False).agg(
            agg_stats
        )

        new_cols = {
            "block": "BSA",
            "fac": "FOL",
            "fenwick": "FA",
            "goal": "GA",
            "hit": "HT",
            "miss": "MSA",
            "penl": "PEND",
            "pen0": "PEND0",
            "pen2": "PEND2",
            "pen4": "PEND4",
            "pen5": "PEND5",
            "pen10": "PEND10",
            "shot": "SA",
            "dzf": "DZFOL",
            "nzf": "NZFOL",
            "ozf": "OZFOL",
            "event_length": "toi",
            "event_team": "opp_team",
            "opp_team": "team",
            "opp_strength_state": "strength_state",
            "opp_score_state": "score_state",
            "opp_score_diff": "score_diff",
            "opp_team_on_id": "teammates_id",
            "opp_team_on": "teammates",
            "opp_team_on_positions": "teammates_positions",
            "opp_team_on_hands": "teammates_hands",
            "opp_team_on_ages": "teammates_ages",
            "opp_team_on_ages_mean": "teammates_ages_mean",
            "opp_team_on_height": "teammates_height",
            "opp_team_on_height_mean": "teammates_height_mean",
            "opp_team_on_weight": "teammates_weight",
            "opp_team_on_weight_mean": "teammates_weight_mean",
            "opp_forwards": "forwards",
            "opp_forwards_id": "forwards_id",
            "opp_forwards_hands": "forwards_hands",
            "opp_forwards_ages": "forwards_ages",
            "opp_forwards_ages_mean": "forwards_ages_mean",
            "opp_forwards_height": "forwards_height",
            "opp_forwards_height_mean": "forwards_height_mean",
            "opp_forwards_weight": "forwards_weight",
            "opp_forwards_weight_mean": "forwards_weight_mean",
            "opp_defense": "defense",
            "opp_defense_id": "defense_id",
            "opp_defense_hands": "defense_hands",
            "opp_defense_ages": "defense_ages",
            "opp_defense_ages_mean": "defense_ages_mean",
            "opp_defense_height": "defense_height",
            "opp_defense_height_mean": "defense_height_mean",
            "opp_defense_weight": "defense_weight",
            "opp_defense_weight_mean": "defense_weight_mean",
            "opp_goalie": "own_goalie",
            "opp_goalie_id": "own_goalie_id",
            "opp_goalie_age": "own_goalie_age",
            "opp_goalie_catches": "own_goalie_catches",
            "opp_goalie_height": "own_goalie_height",
            "opp_goalie_weight": "own_goalie_weight",
            "teammates_id": "opp_team_on_id",
            "teammates": "opp_team_on",
            "teammates_positions": "opp_team_on_positions",
            "teammates_hands": "opp_team_on_hands",
            "teammates_ages": "opp_team_on_ages",
            "teammates_ages_mean": "opp_team_on_ages_mean",
            "teammates_height": "opp_team_on_height",
            "teammates_height_mean": "opp_team_on_height_mean",
            "teammates_weight": "opp_team_on_weight",
            "teammates_weight_mean": "opp_team_on_weight_mean",
            "forwards": "opp_forwards",
            "forwards_id": "opp_forwards_id",
            "forwards_hands": "opp_forwards_hands",
            "forwards_ages": "opp_forwards_ages",
            "forwards_ages_mean": "opp_forwards_ages_mean",
            "forwards_height": "opp_forwards_height",
            "forwards_height_mean": "opp_forwards_height_mean",
            "forwards_weight": "opp_forwards_weight",
            "forwards_weight_mean": "opp_forwards_weight_mean",
            "defense": "opp_defense",
            "defense_id": "opp_defense_id",
            "defense_hands": "opp_defense_hands",
            "defense_ages": "opp_defense_ages",
            "defense_ages_mean": "opp_defense_ages_mean",
            "defense_height": "opp_defense_height",
            "defense_height_mean": "opp_defense_height_mean",
            "defense_weight": "opp_defense_weight",
            "defense_weight_mean": "opp_defense_weight_mean",
            "own_goalie": "opp_goalie",
            "own_goalie_id": "opp_goalie_id",
            "own_goalie_age": "opp_goalie_age",
            "own_goalie_catches": "opp_goalie_catches",
            "own_goalie_height": "opp_goalie_height",
            "own_goalie_weight": "opp_goalie_weight",
        }

        player_cols = dict(
            zip(
                player_cols,
                ["player", "eh_id", "age", "position", "hand", "height", "weight"],
            )
        )

        new_cols.update(player_cols)

        player_df = player_df.rename(columns=new_cols)

        opp_list.append(player_df)

    opp_stats = pd.concat(opp_list, ignore_index=True)

    agg_stats = {x: "sum" for x in opp_stats.columns if x not in merge_columns_for}

    opp_stats = opp_stats.groupby(merge_columns_for, as_index=False, dropna=False).agg(
        agg_stats
    )

    on_ice = event_stats.merge(
        opp_stats, how="outer", on=merge_columns_for, suffixes=("_for", "_against")
    )

    on_ice["toi"] = (on_ice.toi_for + on_ice.toi_against) / 60

    return on_ice
