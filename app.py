from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse
from datetime import datetime
import requests
import os
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import asyncio
import aiohttp

app = FastAPI()

app.mount("/static", StaticFiles(directory="FRONTEND"), name="static")
templates = Jinja2Templates(directory="FRONTEND")

API_FOOTBALL_KEY = '505703178f0eb0be24c37646ea9d06d9'
HEADERS = {'x-apisports-key': API_FOOTBALL_KEY}

async def fetch(session, url):
    async with session.get(url, headers=HEADERS) as response:
        return await response.json()

async def get_team_last_matches(session, team_id, count=30):
    url = f"https://v3.football.api-sports.io/fixtures?team={team_id}&last={count}"
    data = await fetch(session, url)
    return data.get('response', [])

async def get_fixture_statistics(session, fixture_id):
    url = f"https://v3.football.api-sports.io/fixtures/statistics?fixture={fixture_id}"
    data = await fetch(session, url)
    return data.get('response', [])

async def get_h2h_matches(session, team1_id, team2_id):
    url = f"https://v3.football.api-sports.io/fixtures/headtohead?h2h={team1_id}-{team2_id}"
    data = await fetch(session, url)
    return data.get('response', [])

def safe_goal_sum(m):
    home_goals = m.get('goals', {}).get('home') or 0
    away_goals = m.get('goals', {}).get('away') or 0
    return home_goals + away_goals

async def calculate_probability(session, match):
    team1 = match['teams']['home']
    team2 = match['teams']['away']

    team1_stats, team2_stats, h2h_matches = await asyncio.gather(
        get_team_last_matches(session, team1['id']),
        get_team_last_matches(session, team2['id']),
        get_h2h_matches(session, team1['id'], team2['id'])
    )

    def calc_team_percent(stats, home_or_away):
        if not stats:
            return None
        goals = [
            m['goals'][home_or_away] for m in stats
            if m.get('goals') and m['goals'].get(home_or_away) is not None
        ]
        if not goals:
            return None
        scored = sum(1 for g in goals if g >= 1)
        return (scored / len(goals)) * 100 if goals else None

    team1_percent = calc_team_percent(team1_stats, 'home')
    team2_percent = calc_team_percent(team2_stats, 'away')

    h2h_goals = [1 for m in h2h_matches if safe_goal_sum(m) >= 1]
    h2h_percent = (len(h2h_goals) / len(h2h_matches)) * 100 if h2h_matches else None

    last7_team1 = team1_stats[-7:] if len(team1_stats) >= 7 else team1_stats
    last7_team2 = team2_stats[-7:] if len(team2_stats) >= 7 else team2_stats
    combined_7 = last7_team1 + last7_team2
    goals_7 = []
    for m in combined_7:
        goals_data = m.get('goals', {})
        home_goals = goals_data.get('home') or 0
        away_goals = goals_data.get('away') or 0
        if home_goals >= 1 or away_goals >= 1:
            goals_7.append(m)

    last7_percent = (len(goals_7) / len(combined_7)) * 100 if combined_7 else None

    components = []
    weights = []

    if team1_percent is not None:
        components.append(team1_percent * 0.4)
        weights.append(0.4)
    if team2_percent is not None:
        components.append(team2_percent * 0.4)
        weights.append(0.4)
    if h2h_percent is not None:
        components.append(h2h_percent * 0.1)
        weights.append(0.1)
    if last7_percent is not None:
        components.append(last7_percent * 0.1)
        weights.append(0.1)

    final_percent = sum(components) / sum(weights) if weights else 0

    return {
        'league': match['league']['name'],
        'team1': team1['name'],
        'team1_full': team1['name'],
        'team1_percent': round(team1_percent, 2) if team1_percent is not None else 'N/A',
        'team2': team2['name'],
        'team2_full': team2['name'],
        'team2_percent': round(team2_percent, 2) if team2_percent is not None else 'N/A',
        'h2h_percent': round(h2h_percent, 2) if h2h_percent is not None else 'N/A',
        'form_percent': round(last7_percent, 2) if last7_percent is not None else 'N/A',
        'final_percent': round(final_percent, 2)
    }

@app.get("/api/analyze")
async def analyze(type: str, from_date: str, to_date: str):
    async with aiohttp.ClientSession() as session:
        url = f"https://v3.football.api-sports.io/fixtures?date={from_date[:10]}"
        fixtures_data = await fetch(session, url)
        fixtures = fixtures_data.get('response', [])
        tasks = [calculate_probability(session, match) for match in fixtures]
        analysis_results = await asyncio.gather(*tasks)
        return JSONResponse(analysis_results)

@app.post("/api/save-pdf")
async def save_pdf(data: dict):
    file_path = "analysis_results.pdf"
    c = canvas.Canvas(file_path, pagesize=letter)
    width, height = letter
    y = height - 50

    c.setFont("Helvetica-Bold", 16)
    c.drawString(50, y, "Match Analysis Results")
    y -= 30

    c.setFont("Helvetica", 10)

    for match in data["matches"]:
        c.drawString(50, y, f"Liga: {match['league']}")
        y -= 15
        c.drawString(50, y, f"{match['team1']} ({match['team1_full']}) vs {match['team2']} ({match['team2_full']})")
        y -= 15
        c.drawString(60, y, f"{match['team1']}: {match['team1_percent']}% (Last 30 Matches)")
        y -= 15
        c.drawString(60, y, f"{match['team2']}: {match['team2_percent']}% (Last 30 Matches)")
        y -= 15
        c.drawString(60, y, f"H2H: {match['h2h_percent']}% | Form: {match['form_percent']}%")
        y -= 15
        c.drawString(60, y, f"Final Probability: {match['final_percent']}%")
        y -= 25

        if y < 100:
            c.showPage()
            y = height - 50

    c.save()
    return FileResponse(file_path, filename="analysis_results.pdf")

@app.get("/")
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})
