import requests
import time
import random
from datetime import datetime, timedelta

API_KEY = '505703178f0eb0be24c37646ea9d06d9'  # <<< REPLACE WITH YOUR API KEY
BASE_URL = 'https://v3.football.api-sports.io'
HEADERS = {'x-apisports-key': API_KEY}

def rate_limited_request(url, params=None, max_retries=5):
    retries = 0
    while retries <= max_retries:
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', '2'))
            backoff = retry_after + random.uniform(0.5, 1.5)
            print(f"Rate limit hit. Waiting {backoff:.2f}s before retry...")
            time.sleep(backoff)
            retries += 1
        else:
            print(f"Request failed with status {response.status_code}. Retrying...")
            time.sleep(2 ** retries)
            retries += 1
    print(f"Failed after {max_retries} retries.")
    return None

def is_fixture_in_range(fixture_datetime_str, start_dt, end_dt):
    fixture_dt = datetime.fromisoformat(fixture_datetime_str.replace("Z", "+00:00"))
    return start_dt <= fixture_dt <= end_dt

def is_valid_competition(fixture):
    league = fixture["league"]
    league_name = league["name"].lower()
    country = league["country"].lower()

    # 1. Prva i druga nacionalna liga
    if league["type"] == "League" and league["season"] >= 2020:
        if "2" in league_name or "first" in league_name or "premier" in league_name or "championship" in league_name:
            return True

    # 2. Glavni nacionalni kupovi
    if league["type"] == "Cup" and any(x in league_name for x in [
        "fa cup", "coppa italia", "copa del rey", "dfb pokal", "taça de portugal", "coupe de france"
    ]):
        return True

    # 3. Evropa (LS, LE, LK)
    if any(x in league_name for x in ["champions league", "europa league", "conference league"]):
        return True

    # 4. Internacionalna (Južna Amerika / Azija)
    if any(x in league_name for x in ["libertadores", "sudamericana", "afc champions"]):
        return True

    # 5. Reprezentacije
    if league["type"] == "International" or any(x in league_name for x in [
        "world cup", "euro", "uefa nations", "qualifier", "friendly", "african cup", "asian cup"
    ]):
        return True

    return False

def get_fixtures_in_time_range(start_dt_str, end_dt_str):
    start_dt = datetime.fromisoformat(start_dt_str)
    end_dt = datetime.fromisoformat(end_dt_str)

    current_date = start_dt.date()
    end_date = end_dt.date()
    fixtures = []

    while current_date <= end_date:
        params = {
            "date": current_date.isoformat()
        }
        response = requests.get(API_URL, headers=HEADERS, params=params)
        data = response.json()

        for match in data.get("response", []):
            fixture_date_str = match["fixture"]["date"]
            if is_fixture_in_range(fixture_date_str, start_dt, end_dt) and is_valid_competition(match):
                fixtures.append(match)

        current_date += timedelta(days=1)

    return fixtures

def fetch_last_matches_for_teams(fixtures, last_n=30):
    team_ids = set()
    team_last_matches = {}

    for fixture in fixtures:
        home_team_id = fixture['teams']['home']['id']
        away_team_id = fixture['teams']['away']['id']
        team_ids.add(home_team_id)
        team_ids.add(away_team_id)

    print(f"Fetching last {last_n} matches for {len(team_ids)} unique teams...")

    for team_id in team_ids:
        response = rate_limited_request(f"{BASE_URL}/teams/history", params={
            'team': team_id,
            'last': last_n
        })

        if response and 'response' in response:
            team_last_matches[team_id] = response['response']
        else:
            print(f"Failed to fetch last matches for team ID {team_id}")

    return team_last_matches

def fetch_h2h_matches(fixtures, last_n=10):
    h2h_results = {}

    for fixture in fixtures:
        home_team_id = fixture['teams']['home']['id']
        away_team_id = fixture['teams']['away']['id']
        h2h_key = f"{home_team_id}-{away_team_id}"

        if h2h_key in h2h_results:
            continue

        response = rate_limited_request(f"{BASE_URL}/fixtures/headtohead", params={
            'h2h': h2h_key,
            'last': last_n
        })

        if response and 'response' in response:
            h2h_results[h2h_key] = response['response']
        else:
            print(f"Failed to fetch H2H for {h2h_key}")

    return h2h_results

def calculate_team_first_half_goal_percentage(team_matches):
    total_matches = len(team_matches)
    if total_matches == 0:
        return 0

    matches_with_goal_1h = 0
    for match in team_matches:
        goals_home_1h = match['goals']['home']['ht'] if match['goals']['home']['ht'] is not None else 0
        goals_away_1h = match['goals']['away']['ht'] if match['goals']['away']['ht'] is not None else 0

        if (goals_home_1h + goals_away_1h) >= 1:
            matches_with_goal_1h += 1

    percentage = (matches_with_goal_1h / total_matches) * 100
    return round(percentage, 2)

def calculate_h2h_first_half_goal_percentage(h2h_matches):
    total_matches = len(h2h_matches)
    if total_matches == 0:
        return 0

    matches_with_goal_1h = 0
    for match in h2h_matches:
        goals_home_1h = match['goals']['home']['ht'] if match['goals']['home']['ht'] is not None else 0
        goals_away_1h = match['goals']['away']['ht'] if match['goals']['away']['ht'] is not None else 0

        if (goals_home_1h + goals_away_1h) >= 1:
            matches_with_goal_1h += 1

    percentage = (matches_with_goal_1h / total_matches) * 100
    return round(percentage, 2)

def calculate_shot_attack_percentages(team_last_matches):
    team_scores = {}

    for team_id, matches in team_last_matches.items():
        total_shots = 0
        total_attacks = 0
        valid_matches = 0

        for match in matches:
            fixture_id = match['fixture']['id']
            stats_response = rate_limited_request(f"{BASE_URL}/fixtures/statistics", params={
                'fixture': fixture_id
            })

            if not stats_response or 'response' not in stats_response:
                continue

            team_stats = next(
                (item for item in stats_response['response'] if item['team']['id'] == team_id),
                None
            )

            if not team_stats or 'statistics' not in team_stats:
                continue

            shots = None
            attacks = None
            for stat in team_stats['statistics']:
                if stat['type'] == 'Shots on Goal':
                    shots = stat['value']
                elif stat['type'] == 'Dangerous Attacks':
                    attacks = stat['value']

            if isinstance(shots, int) and isinstance(attacks, int):
                total_shots += shots
                total_attacks += attacks
                valid_matches += 1

            time.sleep(1.2)  # API rate limit

        if valid_matches == 0:
            team_scores[team_id] = {
                'avg_shots': 0,
                'avg_attacks': 0,
                'shots_percent': 0,
                'attacks_percent': 0
            }
            continue

        avg_shots = total_shots / valid_matches
        avg_attacks = total_attacks / valid_matches

        # Normalizuj u procentualni skor (max prag: 9 šuteva, 41 napad)
        shots_percent = min(round((avg_shots / 9) * 100, 2), 100)
        attacks_percent = min(round((avg_attacks / 41) * 100, 2), 100)

        team_scores[team_id] = {
            'avg_shots': round(avg_shots, 2),
            'avg_attacks': round(avg_attacks, 2),
            'shots_percent': shots_percent,
            'attacks_percent': attacks_percent
        }

    return team_scores

def get_fixture_details(fixture_id):
    url = f"{API_HOST}/fixtures"
    params = {"id": fixture_id}
    res = requests.get(url, headers=HEADERS, params=params)
    return res.json()["response"][0]

def get_standings(league_id, season):
    url = f"{API_HOST}/standings"
    params = {"league": league_id, "season": season}
    res = requests.get(url, headers=HEADERS, params=params)
    return res.json()["response"][0]["league"]["standings"]

def calculate_match_importance(fixture_id):
    fixture = get_fixture_details(fixture_id)
    league = fixture["league"]
    home_id = fixture["teams"]["home"]["id"]
    away_id = fixture["teams"]["away"]["id"]

    importance_score = 0

    # 1. Takmičenje
    lname = league["name"].lower()
    if any(x in lname for x in ["champions", "europa", "libertadores", "world cup", "nations"]):
        importance_score += 3
    elif any(x in lname for x in ["premier", "la liga", "serie", "bundesliga", "liga", "league"]):
        importance_score += 2
    elif "friendly" in lname:
        importance_score += 0
    else:
        importance_score += 1

    # 2. Faza takmičenja
    round_name = league["round"].lower()
    if any(x in round_name for x in ["final", "semi", "quarter"]):
        importance_score += 3
    elif any(x in round_name for x in ["group", "regular"]):
        importance_score += 1

    # 3. Tabela
    try:
        standings = get_standings(league["id"], league["season"])
        for group in standings:
            for team in group:
                tid = team["team"]["id"]
                rank = team["rank"]
                if tid == home_id or tid == away_id:
                    # Top 3 ili Bottom 3 pozicija daje dodatne poene
                    if rank <= 3:
                        importance_score += 2
                    elif rank >= (len(group) - 2):
                        importance_score += 2
    except:
        pass  # Ako nema tabela (npr. kup), ignoriši

    # Skaliranje na max 10
    return min(importance_score, 10)

    def calculate_final_probability(fixture, team_last_matches, h2h_results, shot_scores):
    home_id = fixture['teams']['home']['id']
    away_id = fixture['teams']['away']['id']
    fixture_id = fixture['fixture']['id']
    h2h_key = f"{home_id}-{away_id}"

    # Komponente i njihove težine
    components = []
    
    # 1. TIM 1 i TIM 2 poslednjih 30 mečeva (60%)
    home_1h_percent = calculate_team_first_half_goal_percentage(team_last_matches.get(home_id, []))
    away_1h_percent = calculate_team_first_half_goal_percentage(team_last_matches.get(away_id, []))
    if home_1h_percent > 0 or away_1h_percent > 0:
        avg_1h_percent = (home_1h_percent + away_1h_percent) / 2
        components.append({"value": avg_1h_percent, "weight": 60})

    # 2. H2H (20%)
    h2h_matches = h2h_results.get(h2h_key, [])
    h2h_1h_percent = calculate_h2h_first_half_goal_percentage(h2h_matches)
    if h2h_1h_percent > 0:
        components.append({"value": h2h_1h_percent, "weight": 20})

    # 3. Šutevi + napadi (15%)
    home_shots = shot_scores.get(home_id, {})
    away_shots = shot_scores.get(away_id, {})

    home_attack_score = (home_shots.get('shots_percent', 0) + home_shots.get('attacks_percent', 0)) / 2
    away_attack_score = (away_shots.get('shots_percent', 0) + away_shots.get('attacks_percent', 0)) / 2

    if home_attack_score > 0 or away_attack_score > 0:
        avg_attack = (home_attack_score + away_attack_score) / 2
        components.append({"value": avg_attack, "weight": 15})

    # 4. Motivacija (5%)
    motivation_score = calculate_match_importance(fixture_id)
    if motivation_score > 0:
        motivation_percent = (motivation_score / 10) * 100
        components.append({"value": motivation_percent, "weight": 5})

    if not components:
        return 0  # Ako su svi 0, vraćamo 0%

    # Dinamička preraspodela težina
    total_weight = sum(c["weight"] for c in components)
    normalized_score = sum((c["value"] * (c["weight"] / total_weight)) for c in components)

    return round(normalized_score, 2)
