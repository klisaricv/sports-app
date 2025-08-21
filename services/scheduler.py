import threading
import time
from datetime import datetime, timedelta

def _seconds_until_next_0001_local(tz):
    now = datetime.now(tz)
    target = now.replace(hour=0, minute=1, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    return max(1, int((target - now).total_seconds()))

def start_scheduler(repo, user_tz, last_n=15, h2h_n=10):
    """
    - Odmah na startu: ensure za današnji dan (idempotentno).
    - Svaku noć u 00:01 lokalno: ensure ponovo za novi dan.
    """
    def _runner():
        try:
            # odmah na bootu — da "danas" nikad ne bude prazan
            repo.ensure_day(
                datetime.now(user_tz).date(),
                last_n=last_n, h2h_n=h2h_n, prewarm_stats=False, prewarm_extras=True
            )
        except Exception as e:
            print(f"[scheduler] initial ensure_day failed: {e}")

        while True:
            try:
                sleep_s = _seconds_until_next_0001_local(user_tz)
                print(f"[scheduler] sleeping {sleep_s}s until 00:01 local…")
                time.sleep(sleep_s)
                # 00:01 lokalno → osiguraj novi dan
                repo.ensure_day(
                    datetime.now(user_tz).date(),
                    last_n=last_n, h2h_n=h2h_n, prewarm_stats=False, prewarm_extras=True
                )
                print("[scheduler] ensure_day done at 00:01")
            except Exception as e:
                print(f"[scheduler] loop error: {e}")
                time.sleep(30)

    t = threading.Thread(target=_runner, name="ensure-day-scheduler", daemon=True)
    t.start()
