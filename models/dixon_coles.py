# models/dixon_coles.py
from __future__ import annotations
import math
from math import exp, log
from dataclasses import dataclass
from typing import Dict, Tuple, List, Optional
import numpy as np
from scipy.optimize import minimize

# ---- DC correction factor ----
def dixon_coles_correction(i: int, j: int, lam_home: float, lam_away: float, rho: float) -> float:
    if i == 0 and j == 0:
        return 1 - (lam_home * lam_away * rho)
    if i == 0 and j == 1:
        return 1 + (lam_home * rho)
    if i == 1 and j == 0:
        return 1 + (lam_away * rho)
    if i == 1 and j == 1:
        return 1 - rho
    return 1.0

@dataclass
class DCParams:
    alpha: float
    home_adv: float
    rho: float
    attack: Dict[int, float]
    defense: Dict[int, float]

def _pack_params(params: DCParams, team_ids: List[int]) -> np.ndarray:
    a = [params.alpha, params.home_adv, params.rho]
    a.extend([params.attack.get(t, 0.0) for t in team_ids])
    a.extend([params.defense.get(t, 0.0) for t in team_ids])
    return np.array(a, dtype=float)

def _unpack_params(x: np.ndarray, team_ids: List[int]) -> DCParams:
    alpha, H, rho = x[0], x[1], x[2]
    n = len(team_ids)
    attack = {team_ids[i]: x[3 + i] for i in range(n)}
    defense = {team_ids[i]: x[3 + n + i] for i in range(n)}
    return DCParams(alpha, H, rho, attack, defense)

def _lambda_home_away(p: DCParams, h: int, a: int) -> Tuple[float,float]:
    att_h = p.attack.get(h, 0.0)
    def_a = p.defense.get(a, 0.0)
    att_a = p.attack.get(a, 0.0)
    def_h = p.defense.get(h, 0.0)
    lam_h = math.exp(p.alpha + att_h - def_a + p.home_adv)
    lam_a = math.exp(p.alpha + att_a - def_h)
    return lam_h, lam_a

def _dc_loglik(x: np.ndarray,
               matches: List[Tuple[int,int,int,int,float]],
               team_ids: List[int],
               ridge: float = 0.001) -> float:
    p = _unpack_params(x, team_ids)
    # identifiability: penalizujemo sumu napada/odbrana ka 0
    atk_sum = sum(p.attack.values())
    def_sum = sum(p.defense.values())
    pen = ridge * (atk_sum**2 + def_sum**2)
    for v in list(p.attack.values()) + list(p.defense.values()):
        pen += ridge * (v**2)
    pen += ridge * (p.alpha**2 + p.home_adv**2 + p.rho**2)

    ll = 0.0
    for h, a, gh, ga, w in matches:
        lam_h, lam_a = _lambda_home_away(p, h, a)
        if lam_h <= 0 or lam_a <= 0:
            return 1e9
        ll_ij = gh * math.log(lam_h) - lam_h - math.lgamma(gh + 1)
        ll_ij += ga * math.log(lam_a) - lam_a - math.lgamma(ga + 1)
        c = dixon_coles_correction(gh, ga, lam_h, lam_a, p.rho)
        if c <= 0:
            return 1e9
        ll_ij += math.log(c)
        ll += w * ll_ij
    return -(ll) + pen

def fit_dc(matches: List[Tuple[int,int,int,int,float]],
           team_ids: List[int],
           init_alpha: float = -0.1,
           init_home_adv: float = 0.2,
           init_rho: float = 0.0,
           ridge: float = 0.001,
           maxiter: int = 500) -> DCParams:
    init = DCParams(
        alpha=init_alpha, home_adv=init_home_adv, rho=init_rho,
        attack={t: 0.0 for t in team_ids},
        defense={t: 0.0 for t in team_ids}
    )
    x0 = _pack_params(init, team_ids)
    res = minimize(
        fun=_dc_loglik,
        x0=x0,
        args=(matches, team_ids, ridge),
        method="L-BFGS-B",
        options={"maxiter": maxiter, "ftol": 1e-6}
    )
    if not res.success:
        x0[0] = -0.05
        x0[1] = 0.15
        res = minimize(
            fun=_dc_loglik, x0=x0, args=(matches, team_ids, ridge),
            method="L-BFGS-B", options={"maxiter": maxiter, "ftol": 1e-6}
        )
    p = _unpack_params(res.x, team_ids)
    return p

def score_matrix(p: DCParams, home_id: int, away_id: int, max_goals: int = 8) -> np.ndarray:
    lam_h, lam_a = _lambda_home_away(p, home_id, away_id)
    mat = np.zeros((max_goals+1, max_goals+1), dtype=float)
    for i in range(max_goals+1):
        for j in range(max_goals+1):
            base = (lam_h**i) * math.exp(-lam_h) / math.factorial(i)
            base *= (lam_a**j) * math.exp(-lam_a) / math.factorial(j)
            c = dixon_coles_correction(i, j, lam_h, lam_a, p.rho)
            mat[i, j] = base * c
    s = mat.sum()
    if s <= 0:
        for i in range(max_goals+1):
            for j in range(max_goals+1):
                base = (lam_h**i) * math.exp(-lam_h) / math.factorial(i)
                base *= (lam_a**j) * math.exp(-lam_a) / math.factorial(j)
                mat[i, j] = base
        s = mat.sum()
    return mat / s

def probs_from_matrix(M: np.ndarray) -> Dict[str, float]:
    home = float(np.triu(M, 1).sum())
    draw = float(np.trace(M))
    away = float(np.tril(M, -1).sum())
    return {"home": home, "draw": draw, "away": away}

def prob_over_under(M: np.ndarray, line: float) -> Dict[str, float]:
    max_g = M.shape[0] - 1
    p_over = 0.0
    p_under = 0.0
    thr = int(math.floor(line))
    if abs(line - (thr + 0.5)) < 1e-9:
        for i in range(max_g+1):
            for j in range(max_g+1):
                if (i + j) >= (thr + 1):
                    p_over += M[i, j]
                else:
                    p_under += M[i, j]
        return {"over": float(p_over), "under": float(p_under)}
    else:
        p_exact = 0.0
        for i in range(max_g+1):
            for j in range(max_g+1):
                s = i + j
                if s > line:
                    p_over += M[i, j]
                elif s < line:
                    p_under += M[i, j]
                else:
                    p_exact += M[i, j]
        return {"over": float(p_over), "under": float(p_under), "push": float(p_exact)}

def prob_asian_handicap(M: np.ndarray, handicap: float) -> Dict[str, float]:
    max_g = M.shape[0] - 1
    diffs = {}
    for i in range(max_g+1):
        for j in range(max_g+1):
            d = i - j
            diffs[d] = diffs.get(d, 0.0) + float(M[i, j])
    def p_home_win_line(h: float) -> Dict[str, float]:
        win = 0.0; lose = 0.0; push = 0.0
        for d, p in diffs.items():
            val = d + h
            if val > 1e-12:
                win += p
            elif val < -1e-12:
                lose += p
            else:
                push += p
        return {"win": win, "lose": lose, "push": push}
    # quarter lines → prosečna dva susedna
    if abs(handicap - round(handicap*2)/2) < 1e-12 and abs(handicap*2 - int(handicap*2)) > 1e-12:
        h1 = math.floor(handicap*2)/2.0
        h2 = h1 + 0.25
        p1 = p_home_win_line(h1)
        p2 = p_home_win_line(h2)
        return {k: 0.5*(p1[k] + p2[k]) for k in ("win","lose","push")}
    else:
        return p_home_win_line(handicap)
