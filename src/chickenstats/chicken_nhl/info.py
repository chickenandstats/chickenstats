from io import BytesIO
from pathlib import Path
from PIL import Image

from chickenstats.utilities import ChickenSession

correct_names_dict = {
    "AJ GREER": "A.J. GREER",
    "ALEXEY TOROPCHENKO": "ALEXEI TOROPCHENKO",
    "ANDR BENOT": "ANDRE BENOIT",
    "ANTHONY DEANGELO": "TONY DEANGELO",
    "BJ CROMBEEN": "B.J. CROMBEEN",
    "BO GROULX": "BENOIT-OLIVIER GROULX",
    "BRADLEY MILLS": "BRAD MILLS",
    "CAL PETERSEN": "CALVIN PETERSEN",
    "CALLAN FOOTE": "CAL FOOTE",
    "CAM HILLIS": "CAMERON HILLIS",
    "CHASE DELEO": "CHASE DE LEO",
    "CHRIS VANDE VELDE": "CHRIS VANDEVELDE",
    "CRISTOVAL NIEVES": "BOO NIEVES",
    "DAN CLEARY": "DANIEL CLEARY",
    "DANNY CLEARY": "DANIEL CLEARY",
    "DANIEL CARCILLO": "DAN CARCILLO",
    "DANNY BRIERE": "DANIEL BRIERE",
    "DANNY O'REGAN": "DANIEL O'REGAN",
    "DAVID JOHNNY ODUYA": "JOHNNY ODUYA",
    "EVGENII DADONOV": "EVGENY DADONOV",
    "EGOR SHARANGOVICH": "YEGOR SHARANGOVICH",
    "FREDDY MODIN": "FREDRIK MODIN",
    "FREDERICK MEYER IV": "FREDDY MEYER",
    "GERRY MAYHEW": "GERALD MAYHEW",
    "HARRISON ZOLNIERCZYK": "HARRY ZOLNIERCZYK",
    "JAMES WYMAN": "J.T. WYMAN",
    "JT WYMAN": "J.T. WYMAN",
    "JEAN-FRANCOIS BERUBE": "J-F BERUBE",
    "J.F. BERUBE": "J-F BERUBE",
    "J.J. MOSER": "JANIS MOSER",
    "JAKE MIDDLETON": "JACOB MIDDLETON",
    "JEAN-FRANCOIS JACQUES": "J-F JACQUES",
    "JONATHAN AUDY-MARCHESSAULT": "JONATHAN MARCHESSAULT",
    "JOSH DUNNE": "JOSHUA DUNNE",
    "JOSHUA MORRISSEY": "JOSH MORRISSEY",
    "JT BROWN": "J.T. BROWN",
    "JT COMPHER": "J.T. COMPHER",
    "KENNETH APPLEBY": "KEN APPLEBY",
    "KRYSTOFER BARCH": "KRYS BARCH",
    "MARTIN ST LOUIS": "MARTIN ST. LOUIS",
    "MARTIN ST PIERRE": "MARTIN ST. PIERRE",
    "MARTY HAVLAT": "MARTIN HAVLAT",
    "MATHEW DUMBA": "MATT DUMBA",
    "MATTHEW DUMBA": "MATT DUMBA",
    "MATTHEW BENNING": "MATT BENNING",
    "MATTHEW CARLE": "MATT CARLE",
    "MATTHEW IRWIN": "MATT IRWIN",
    "MATTHEW MURRAY": "MATT MURRAY",
    "MATTHEW NIETO": "MATT NIETO",
    "MATTIAS JANMARK-NYLEN": "MATTIAS JANMARK",
    "MAXIME TALBOT": "MAX TALBOT",
    "MAX LAJOIE": "MAXIME LAJOIE",
    "MAXWELL REINHART": "MAX REINHART",
    "MICHAEL CAMMALLERI": "MIKE CAMMALLERI",
    "MICHAEL GRIER": "MIKE GRIER",
    "MICHAEL FERLAND": "MICHEAL FERLAND",
    "MICHAEL MATHESON": "MIKE MATHESON",
    "MICHAEL RUPP": "MIKE RUPP",
    "MICHAEL SANTORELLI": "MIKE SANTORELLI",
    "MICHAEL YORK": "MIKE YORK",
    "MIKE ZIGOMANIS": "MICHAEL ZIGOMANIS",
    "MIKE VERNACE": "MICHAEL VERNACE",
    "MITCHELL MARNER": "MITCH MARNER",
    "NICOLAS PETAN": "NIC PETAN",
    "NICHOLAS BAPTISTE": "NICK BAPTISTE",
    "NICHOLAS BOYNTON": "NICK BOYNTON",
    "NICHOLAS CAAMANO": "NICK CAAMANO",
    "NICHOLAS DRAZENOVIC": "NICK DRAZENOVIC",
    "NICHOLAS PAUL": "NICK PAUL",
    "NICHOLAS SHORE": "NICK SHORE",
    "NICK ABRUZZESE": "NICHOLAS ABRUZZESE",
    "NICK MERKLEY": "NICHOLAS MERKLEY",
    "NICKLAS GROSSMAN": "NICKLAS GROSSMANN",
    "NIKLAS KRONVALL": "NIKLAS KRONWALL",
    "NIKOLAI KULEMIN": "NIKOLAY KULEMIN",
    "OLIVIER MAGNAN-GRENIER": "OLIVIER MAGNAN",
    "PA PARENTEAU": "P.A. PARENTEAU",
    "PIERRE-ALEX PARENTEAU": "P.A. PARENTEAU",
    "PAT MAROON": "PATRICK MAROON",
    "PHILIP VARONE": "PHIL VARONE",
    "QUINTIN HUGHES": "QUINN HUGHES",
    "RJ UMBERGER": "R.J. UMBERGER",
    "SAMMY WALKER": "SAMUEL WALKER",
    "SASHA CHMELEVSKI": "ALEX CHMELEVSKI",
    "STEVEN REINPRECHT": "STEVE REINPRECHT",
    "THOMAS MCCOLLUM": "TOM MCCOLLUM",
    "TIM GETTINGER": "TIMOTHY GETTINGER",
    "TJ GALIARDI": "T.J. GALIARDI",
    "TJ HENSICK": "T.J. HENSICK",
    "TJ OSHIE": "T.J. OSHIE",
    "TJ TYNAN": "T.J. TYNAN",
    "TOBY ENSTROM": "TOBIAS ENSTROM",
    "TOMMY NOVAK": "THOMAS NOVAK",  # API ID: 8478438
    "VINCENT HINOSTROZA": "VINNIE HINOSTROZA",
    "WILL BORGEN": "WILLIAM BORGEN",
    "WILLIAM THOMAS": "BILL THOMAS",
    "ZACHARY ASTON-REESE": "ZACH ASTON-REESE",
    "ZACHARY HAYES": "ZACK HAYES",
    "ZACHARY SANFORD": "ZACH SANFORD",
}

correct_api_names_dict = {
    8480222: "SEBASTIAN.AHO2",
    8476979: "ERIK.GUSTAFSSON2",
    8478400: "COLIN.WHITE2",
    8474744: "SEAN.COLLINS2",
    8471221: "ALEX.PICARD2",
    8482247: "MIKKO.LEHTONEN2",
    8480979: "NATHAN.SMITH2",
    8480193: "DANIIL.TARASOV2",
}

team_codes = {
    "ANAHEIM DUCKS": "ANA",
    "ARIZONA COYOTES": "ARI",
    "ATLANTA FLAMES": "AFM",
    "ATLANTA THRASHERS": "ATL",
    "BOSTON BRUINS": "BOS",
    "BROOKLYN AMERICANS": "BRK",
    "BUFFALO SABRES": "BUF",
    "CALGARY FLAMES": "CGY",
    "CALGARY TIGERS": "CAT",
    "CALIFORNIA GOLDEN SEALS": "CGS",
    "CANADA": "CAN",
    "CAROLINA HURRICANES": "CAR",
    "CHICAGO BLACKHAWKS": "CHI",
    "CLEVELAND BARONS": "CLE",
    "COLORADO AVALANCHE": "COL",
    "COLORADO ROCKIES": "CLR",
    "COLUMBUS BLUE JACKETS": "CBJ",
    "DALLAS STARS": "DAL",
    "DETROIT COUGARS": "DCG",
    "DETROIT FALCONS": "DFL",
    "DETROIT RED WINGS": "DET",
    "EDMONTON ESKIMOS": "EDE",
    "EDMONTON OILERS": "EDM",
    "FINLAND": "FIN",
    "FLORIDA PANTHERS": "FLA",
    "HAMILTON TIGERS": "HAM",
    "HARTFORD WHALERS": "HFD",
    "KANSAS CITY SCOUTS": "KCS",
    "LOS ANGELES KINGS": "LAK",
    "MINNESOTA NORTH STARS": "MNS",
    "MINNESOTA WILD": "MIN",
    "MONTREAL CANADIENS": "MTL",
    "MONTREAL MAROONS": "MMR",
    "MONTREAL WANDERERS": "MWN",
    "NASHVILLE PREDATORS": "NSH",
    "NEW JERSEY DEVILS": "NJD",
    "NEW YORK AMERICANS": "NYA",
    "NEW YORK ISLANDERS": "NYI",
    "NEW YORK RANGERS": "NYR",
    "OAKLAND SEALS": "OAK",
    "OTTAWA SENATORS": "OTT",
    "OTTAWA SENATORS (1917)": "SEN",
    "PHILADELPHIA FLYERS": "PHI",
    "PHILADELPHIA QUAKERS": "QUA",
    "PITTSBURGH PENGUINS": "PIT",
    "PITTSBURGH PIRATES": "PIR",
    "QUEBEC BULLDOGS": "QBD",
    "QUEBEC NORDIQUES": "QUE",
    "SAN JOSE SHARKS": "SJS",
    "SEATTLE KRAKEN": "SEA",
    "SEATTLE METROPOLITANS": "SEA",
    "ST. LOUIS BLUES": "STL",
    "ST. LOUIS EAGLES": "SLE",
    "SWEDEN": "SWE",
    "TAMPA BAY LIGHTNING": "TBL",
    "TORONTO ARENAS": "TAN",
    "TORONTO MAPLE LEAFS": "TOR",
    "TORONTO ST. PATRICKS": "TSP",
    "UNITED STATES": "USA",
    "UTAH HOCKEY CLUB": "UTA",
    "VANCOUVER CANUCKS": "VAN",
    "VANCOUVER MAROONS": "VMA",
    "VANCOUVER MILLIONAIRES": "VMI",
    "VEGAS GOLDEN KNIGHTS": "VGK",
    "VICTORIA COUGARS": "VIC",
    "WASHINGTON CAPITALS": "WSH",
    "WINNIPEG JETS": "WPG",
    "WINNIPEG JETS (1979)": "WIN",
}

team_names = {
    "ANA": "ANAHEIM DUCKS",
    "ARI": "ARIZONA COYOTES",
    "AFM": "ATLANTA FLAMES",
    "ATL": "ATLANTA THRASHERS",
    "BOS": "BOSTON BRUINS",
    "BRK": "BROOKLYN AMERICANS",
    "BUF": "BUFFALO SABRES",
    "CAN": "CANADA",
    "CGY": "CALGARY FLAMES",
    "CAT": "CALGARY TIGERS",
    "CGS": "CALIFORNIA GOLDEN SEALS",
    "CAR": "CAROLINA HURRICANES",
    "CHI": "CHICAGO BLACKHAWKS",
    "CLE": "CLEVELAND BARONS",
    "COL": "COLORADO AVALANCHE",
    "CLR": "COLORADO ROCKIES",
    "CBJ": "COLUMBUS BLUE JACKETS",
    "DAL": "DALLAS STARS",
    "DCG": "DETROIT COUGARS",
    "DFL": "DETROIT FALCONS",
    "DET": "DETROIT RED WINGS",
    "EDE": "EDMONTON ESKIMOS",
    "EDM": "EDMONTON OILERS",
    "FIN": "FINLAND",
    "FLA": "FLORIDA PANTHERS",
    "HAM": "HAMILTON TIGERS",
    "HFD": "HARTFORD WHALERS",
    "KCS": "KANSAS CITY SCOUTS",
    "LAK": "LOS ANGELES KINGS",
    "MNS": "MINNESOTA NORTH STARS",
    "MIN": "MINNESOTA WILD",
    "MTL": "MONTREAL CANADIENS",
    "MMR": "MONTREAL MAROONS",
    "MWN": "MONTREAL WANDERERS",
    "NSH": "NASHVILLE PREDATORS",
    "NJD": "NEW JERSEY DEVILS",
    "NYA": "NEW YORK AMERICANS",
    "NYI": "NEW YORK ISLANDERS",
    "NYR": "NEW YORK RANGERS",
    "OAK": "OAKLAND SEALS",
    "OTT": "OTTAWA SENATORS",
    "SEN": "OTTAWA SENATORS (1917)",
    "PHI": "PHILADELPHIA FLYERS",
    "QUA": "PHILADELPHIA QUAKERS",
    "PIT": "PITTSBURGH PENGUINS",
    "PIR": "PITTSBURGH PIRATES",
    "QBD": "QUEBEC BULLDOGS",
    "QUE": "QUEBEC NORDIQUES",
    "SJS": "SAN JOSE SHARKS",
    "SEA": "SEATTLE KRAKEN",
    "STL": "ST. LOUIS BLUES",
    "SLE": "ST. LOUIS EAGLES",
    "SWE": "SWEDEN",
    "TBL": "TAMPA BAY LIGHTNING",
    "TAN": "TORONTO ARENAS",
    "TOR": "TORONTO MAPLE LEAFS",
    "TSP": "TORONTO ST. PATRICKS",
    "USA": "UNITED STATES",
    "UTA": "UTAH HOCKEY CLUB",
    "VAN": "VANCOUVER CANUCKS",
    "VMA": "VANCOUVER MAROONS",
    "VMI": "VANCOUVER MILLIONAIRES",
    "VGK": "VEGAS GOLDEN KNIGHTS",
    "VIC": "VICTORIA COUGARS",
    "WSH": "WASHINGTON CAPITALS",
    "WPG": "WINNIPEG JETS",
    "WIN": "WINNIPEG JETS (1979)",
}

alt_team_codes = {"L.A": "LAK", "N.J": "NJD", "S.J": "SJS", "T.B": "TBL", "PHX": "ARI"}

NHL_COLORS = {
    "ANA": {"GOAL": "#F47A38", "SHOT": "#000000", "MISS": "#D3D3D3"},
    "ATL": {"GOAL": "#5C88DA", "SHOT": "#041E42", "MISS": "#D3D3D3"},
    # 'ARI': {'GOAL': '#E2D6B5', 'SHOT': '#8C2633', 'MISS': '#D3D3D3'},
    "ARI": {"GOAL": "#A9431E", "SHOT": "#5F259F", "MISS": "#D3D3D3"},
    "BOS": {"GOAL": "#FFB81C", "SHOT": "#000000", "MISS": "#D3D3D3"},
    "BUF": {"GOAL": "#FCB514", "SHOT": "#002654", "MISS": "#D3D3D3"},
    "CAR": {"GOAL": "#CC0000", "SHOT": "#000000", "MISS": "#D3D3D3"},
    "CBJ": {"GOAL": "#CE1126", "SHOT": "#002654", "MISS": "#D3D3D3"},
    "CGY": {"GOAL": "#F1BE48", "SHOT": "#C8102E", "MISS": "#D3D3D3"},
    "CHI": {"GOAL": "#CF0A2C", "SHOT": "#000000", "MISS": "#D3D3D3"},
    "COL": {"GOAL": "#236192", "SHOT": "#6F263D", "MISS": "#D3D3D3"},
    "DAL": {"GOAL": "#006847", "SHOT": "#111111", "MISS": "#D3D3D3"},
    "DET": {"GOAL": "#FFFFFF", "SHOT": "#CE1126", "MISS": "#D3D3D3"},
    "EDM": {"GOAL": "#FF4C00", "SHOT": "#041E42", "MISS": "#D3D3D3"},
    "FLA": {"GOAL": "#C8102E", "SHOT": "#041E42", "MISS": "#D3D3D3"},
    "LAK": {"GOAL": "#A2AAAD", "SHOT": "#111111", "MISS": "#D3D3D3"},
    "MIN": {"GOAL": "#A6192E", "SHOT": "#154734", "MISS": "#D3D3D3"},
    "MTL": {"GOAL": "#AF1E2D", "SHOT": "#192168", "MISS": "#D3D3D3"},
    "NJD": {"GOAL": "#CE1126", "SHOT": "#000000", "MISS": "#D3D3D3"},
    "NSH": {"GOAL": "#FFB81C", "SHOT": "#041E42", "MISS": "#D3D3D3"},
    "NYI": {"GOAL": "#F47D30", "SHOT": "#00539B", "MISS": "#D3D3D3"},
    "NYR": {"GOAL": "#CE1126", "SHOT": "#0038A8", "MISS": "#D3D3D3"},
    "OTT": {"GOAL": "#C2912C", "SHOT": "#C52032", "MISS": "#D3D3D3"},
    "PHI": {"GOAL": "#F74902", "SHOT": "#000000", "MISS": "#D3D3D3"},
    "PIT": {"GOAL": "#FCB514", "SHOT": "#000000", "MISS": "#D3D3D3"},
    "SEA": {"GOAL": "#99D9D9", "SHOT": "#001628", "MISS": "#D3D3D3"},
    "SJS": {"GOAL": "#006D75", "SHOT": "#000000", "MISS": "#D3D3D3"},
    "STL": {"GOAL": "#FCB514", "SHOT": "#002F87", "MISS": "#D3D3D3"},
    "TBL": {"GOAL": "#FFFFFF", "SHOT": "#002868", "MISS": "#D3D3D3"},
    "TOR": {"GOAL": "#FFFFFF", "SHOT": "#00205B", "MISS": "#D3D3D3"},
    "UTA": {"GOAL": "#6CACE4", "SHOT": "#010101", "MISS": "#D3D3D3"},
    "VAN": {"GOAL": "#00843D", "SHOT": "#00205B", "MISS": "#D3D3D3"},
    "VGK": {"GOAL": "#B4975A", "SHOT": "#333F42", "MISS": "#D3D3D3"},
    "WSH": {"GOAL": "#C8102E", "SHOT": "#041E42", "MISS": "#D3D3D3"},
    "WPG": {"GOAL": "#AC162C", "SHOT": "#041E42", "MISS": "#D3D3D3"},
}

INTERNATIONAL_COLORS = {
    "CAN": {"GOAL": "#CC3333", "SHOT": "#000000", "MISS": "#D3D3D3"},
    "FIN": {"GOAL": "#FBBF16", "SHOT": "#0F80CC", "MISS": "#D3D3D3"},
    "SWE": {"GOAL": "#FCD116", "SHOT": "#3063AE", "MISS": "#D3D3D3"},
    "USA": {"GOAL": "#BB2533", "SHOT": "#1F2742", "MISS": "#D3D3D3"},
}


class Team:
    """Class instance for team information, including team name, code, and colors."""

    def __init__(self, team_code: str | None = None, team_name: str | None = None):
        """Instantiates team information, including team name, code, and colors."""
        if not team_code and not team_name:
            raise ValueError("Either team code or team name must be provided.")

        if team_code and team_code not in team_names.keys() and team_codes not in alt_team_codes.keys():
            raise ValueError(f"Team code {team_code} is not valid.")

        if team_name and team_name not in team_codes.keys():
            raise ValueError(f"Team name {team_name} is not valid.")

        if not team_code:
            team_code = team_codes[team_name]

        if not team_name:
            team_code_alt = f"{team_code}"

            if team_code in alt_team_codes.keys():
                team_code = alt_team_codes[team_code]

            team_name = team_names[team_code]

        self.team_code = team_code
        self.team_code_alt = team_code_alt
        self.team_name = team_name

        if team_code in NHL_COLORS.keys():
            self.colors = NHL_COLORS[team_code]
            folder_stem = "nhl"

        elif team_code in INTERNATIONAL_COLORS.keys():
            self.colors = INTERNATIONAL_COLORS[team_code]
            folder_stem = "international"

        self.primary_color = self.colors["GOAL"]
        self.secondary_color = self.colors["SHOT"]
        self.tertiary_color = self.colors["MISS"]

        if team_code == "ARI":
            self.colors_alt = {"GOAL": "#E2D6B5", "SHOT": "#8C2633", "MISS": "#D3D3D3"}
            self.primary_color_alt = self.colors_alt["GOAL"]
            self.secondary_color_alt = self.colors_alt["SHOT"]
            self.tertiary_color_alt = self.colors_alt["MISS"]

        url_stem = "https://raw.githubusercontent.com/chickenandstats/chickenstats/refs/heads/main/logos"
        self.logo_url = f"{url_stem}/{folder_stem}/{team_code}.png"

    @property
    def logo(self):
        """Fetch logo from chickenstats github repo."""
        with ChickenSession() as session:
            logo = BytesIO(session.get(self.logo_url).content)

            logo = Image.open(logo)

            return logo
