import pandas as pd
import numpy as np
import time

import requests
from bs4 import BeautifulSoup
import re
import unicodedata

from tqdm.notebook import tqdm


def convert_to_list(obj):
    """If the object is not a list, converts the object to a list of length one"""

    if type(obj) is not list and type(obj) is not pd.Series:

        try:

            obj = list(obj)

        except:

            obj = [obj]

    return obj


def munge_cf(df, scrape_year):

    new_cols = {
        x: x.lower().replace(" ", "_").replace("._", "_").replace(".", "_")
        for x in df.columns
    }

    df.rename(columns=new_cols, inplace=True)

    df.player = df.player.replace("(\d+\.)", "", regex=True).str.strip()

    df.player = (
        df.player.str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
    )

    split = df.drafted.astype(str).str.split("-", n=3, expand=True)
    df["pick"] = split[0].str.strip()
    df["round"] = split[1].str.strip()
    df["draft_year (Team)"] = split[2].str.strip()

    split = df["draft_year (Team)"].str.split(" ", n=2, expand=True)
    df["draft_year"] = split[0].str.strip()
    df["draft_team"] = split[1].str.strip()
    df["draft_team"] = (
        df["draft_team"]
        .str.replace("(", "", regex=False)
        .str.replace(")", "", regex=False)
    )

    split = df["date_of_birth"].str.split(",", n=2, expand=True)
    df["birth_year"] = split[1].str.strip()

    cols = [
        "cap_hit",
        "cap_hit_%",
        "aav",
        "salary",
        "base_salary",
        "s_bonus",
        "p_bonus",
        "minors",
    ]

    for col in cols:
        df[col] = (
            df[col]
            .str.replace("$", "", regex=False)
            .str.replace(",", "", regex=False)
            .str.replace("%", "", regex=False)
        )
        df[col] = pd.to_numeric(df[col])  # .convert_dtypes()

    # display(response_df)

    player_split = df.player.str.split(" ", n=1, expand=True)

    alexs = ["ALEXANDER", "ALEXANDRE"]

    for alex in alexs:

        player_split[0] = player_split[0].str.upper().replace(alex, "ALEX", regex=True)

    player_split[0] = player_split[0].str.upper().replace("JOSHUA", "JOSH", regex=True)
    player_split[0] = player_split[0].str.upper().replace("SAMUEL", "SAM", regex=True)
    player_split[0] = player_split[0].str.upper().replace("JOSEPH", "JOE", regex=True)

    df.player = player_split[0].str.upper() + "." + player_split[1].str.upper()

    names_dict = {
        "MITCHELL.MARNER": "MITCH.MARNER",
        "JOSHUA.MORRISSEY": "JOSH.MORRISSEY",
        "MATTHEW.BOLDY": "MATT.BOLDY",
        "DANIEL.VLADAR": "DAN.VLADAR",
        "ANTHONY.DEANGELO": "TONY.DEANGELO",
        "MICHAEL.MATHESON": "MIKE.MATHESON",
        "NICHOLAS.PAUL": "NICK.PAUL",
        "CHRISTOPHER.TANEV": "CHRIS.TANEV",
        "J.J..MOSER": "JANIS.MOSER",
        "MATTHEW.BENNING": "MATT.BENNING",
        "EVGENI.DADONOV": "EVGENY.DADONOV",
        "MAXIME.COMTOIS": "MAX.COMTOIS",
        "JOHN-JASON.PETERKA": "J.J.PETERKA",
        "JOSHUA.NORRIS": "JOSH.NORRIS",
        "MATTHEW.BENIERS": "MATTY.BENIERS",
        "ZACHARY.WERENSKI": "ZACH.WERENSKI",
        "JOSHUA.BROWN": "JOSH.BROWN",
        "SAMUEL.MONTEMBEAULT": "SAM.MONTEMBEAULT",
        "JOSHUA.MAHURA": "JOSHUA.MAHURA",
        "ALEXANDER.KERFOOT": "ALEX.KERFOOT",
        "ALEXANDER.WENNBERG": "ALEX.WENNBERG",
        "AJ.GREER": "A.J.GREER",
        "SAM.GIRARD": "SAMUEL.GIRARD",
        "MICHAEL.ANDERSON": "MIKEY.ANDERSON",
        "CAL.PETERSEN": "CALVIN.PETERSEN",
        "SAM.ERSSON": "SAMUEL.ERSSON",
        "ZACHARY.JONES": "ZAC.JONES",
        "JACOB.CHRISTIANSEN": "JAKE.CHRISTIANSEN",
        "ALEXEY.TOROPCHENKO": "ALEXEI.TOROPCHENKO",
        "YEGOR.ZAMULA": "EGOR.ZAMULA",
        "ZACHARY.SANFORD": "ZACH.SANFORD",
        "SAM.BLAIS": "SAMMY.BLAIS",
        "NICOLAS.PETAN": "NIC.PETAN",
        "SAM.WALKER": "SAMUEL.WALKER",
        "JOE.CRAMAROSSA": "JOSEPH.CRAMAROSSA",
        "DANIEL.RENOUF": "DAN.RENOUF",
        "C.J..SUESS": "CJ.SUESS",
        "SAM.FAGEMO": "SAMUEL.FAGEMO",
        "WILL.BITTEN": "WILLIAM.BITTEN",
    }

    for old_name, new_name in names_dict.items():

        df.player = df.player.str.replace(old_name, new_name, regex=False)

    DUOS = {
        "SEBASTIAN.AHO": df.pos.str.contains("D"),
        "COLIN.WHITE": df.season >= 20162017,
        "SEAN.COLLINS": df.season >= 20162017,
        "ALEX.PICARD": ~df.pos.str.contains("D"),
        "ERIK.GUSTAFSSON": df.season >= 20152016,
        "MIKKO.LEHTONEN": df.season >= 20202021,
        "NATHAN.SMITH": df.season >= 20212022,
        "DANIIL.TARASOV": df.pos == "G",
    }

    DUOS = [
        np.logical_and(df.player == player, condition)
        for player, condition in DUOS.items()
    ]

    df["player_id"] = np.where(np.logical_or.reduce(DUOS), df.player + "2", df.player)

    df["weight_lbs"] = (
        df.weight.str.split("-", expand=True)[0].str.replace(" lbs", "").astype(int)
    )

    df["weight_kg"] = (
        df.weight.str.split("-", expand=True)[1].str.replace(" kg", "").astype(int)
    )

    df["height_imperial"] = df.height.str.split("-", expand=True)[0]
    df["height_cm"] = df.height.str.split("-", expand=True)[1].str.replace(" cm", "")

    df["years_remaining"] = df.exp_year - scrape_year

    df["years_remaining_pct"] = df.years_remaining / df.length

    cols = [
        "extension",
        "slide_cand_",
        "arb_req",
        "arb_elig_1",
        "waivers_exempt",
    ]

    cols = [x for x in cols if x in df.columns]

    for col in cols:

        df[col] = np.where(pd.notnull(df[col]), "yes", "no")

    cols = [
        "season",
        "player",
        "player_id",
        "team",
        "pos",
        "country",
        "date_of_birth",
        "birth_year",
        "age",
        "handed",
        "height",
        "height_imperial",
        "height_cm",
        "weight",
        "weight_lbs",
        "weight_kg",
        "drafted",
        "draft_year",
        "draft_team",
        "pick",
        "round",
        "signing_date",
        "signing_age",
        "signing",
        "expiry",
        "exp_year",
        "years_remaining",
        "pct_remaining",
        "length",
        "extension",
        "type",
        "aav",
        "salary",
        "base_salary",
        "cap_hit",
        "cap_hit_%",
        "s_bonus",
        "p_bonus",
        "clause",
        "arb_req",
        "arb_elig_1",
        "waivers_exempt",
        "minors",
        "slide_cand_",
    ]

    cols = [x for x in cols if x in df.columns]

    rename_cols = {
        "pick": "draft_pick",
        "round": "draft_round",
        "exp_year": "expiration_year",
        "cap_hit_%": "cap_hit_pct",
        "p_bonus": "performance_bonus",
        "s_bonus": "signing_bonus",
        "arb_req": "arbitration_required",
        "arb_elig_1": "arbitration_eligible",
        "pos": "position",
        "length": "contract_length",
        "slide_cand_": "slide_candidate",
        "extension": "contract_extension",
    }

    df = df[cols].rename(columns=rename_cols).drop_duplicates()

    return df


def scrape_cf(years, status=["active"], disable_print=False):
    """
    Function to scrape CapFriendly data
    """

    years = convert_to_list(years)

    CONCAT_LIST = list()

    for year in tqdm(years, desc="Scraping CapFriendly", disable=disable_print):

        scrape_year = year + 1

        season = int(f"{year}{year+1}")

        pages = range(1, 51)

        concat_list = list()

        desc = f"Scraping {year}-{scrape_year} season"

        for page in tqdm(pages, desc=desc, disable=disable_print):

            for player_status in status:

                url = (
                    f"https://www.capfriendly.com/browse/{player_status}/{scrape_year}"
                )

                display_param = (
                    "birthday,country,weight,height,weightkg,heightcm,draft,slide-candidate,"
                    "waivers-exempt,signing-status,expiry-year,performance-bonus,signing-bonus,caphit-percent,aav,"
                    "length,minors-salary,base-salary,arbitration-eligible,type,signing-age,signing-date,arbitration,extension"
                )

                hide_param = "skater-stats,goalie-stats"

                payload = {
                    "age-calculation-date": "october1",
                    "display": display_param,
                    "hide": hide_param,
                    "pg": str(page),
                }

                response = requests.get(url, params=payload)

                if response.status_code != 200:

                    continue

                soup = BeautifulSoup(response.text, "lxml")

                try:

                    # print(soup)
                    response_df = pd.read_html(
                        str(soup.find_all("table")), na_values="-"
                    )[0]

                    if response_df.empty:

                        continue

                    response_df["season"] = season

                    concat_list.append(response_df)

                except:

                    continue

        year_df = pd.concat(concat_list, ignore_index=True)

        year_df = munge_cf(year_df, scrape_year=scrape_year)

        CONCAT_LIST.append(year_df)

    cf = pd.concat(CONCAT_LIST, ignore_index=True)

    return cf
