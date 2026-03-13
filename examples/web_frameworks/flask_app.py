"""
Flask + Atomix STM Integration Example

This example demonstrates how to use Atomix STM in a traditional synchronous
web framework like Flask. STM is particularly useful here for managing
global application state (e.g., a shared leaderboard or configuration)
without the complexity of manual mutex management.
"""

from flask import Flask, jsonify, request
from atomix_stm import Atom, dosync
import threading
import time
import random

app = Flask(__name__)

# Shared state: A high-score leaderboard
# Using a single Atom for the whole leaderboard ensures total consistency.
leaderboard = Atom([
    {"name": "Alice", "score": 100},
    {"name": "Bob", "score": 80},
    {"name": "Charlie", "score": 60}
])

@app.route("/leaderboard", methods=["GET"])
def get_leaderboard():
    return jsonify(leaderboard.deref())

@app.route("/score", methods=["POST"])
def submit_score():
    data = request.json
    name = data.get("name")
    score = data.get("score")
    
    if not name or score is None:
        return jsonify({"error": "Invalid data"}), 400

    def update_leaderboard_tx():
        current = leaderboard.deref()
        # Add new score
        new_list = current + [{"name": name, "score": score}]
        # Sort and keep top 10
        new_list.sort(key=lambda x: x["score"], reverse=True)
        leaderboard.reset(new_list[:10])
        return {"status": "updated", "rank": next(i for i, x in enumerate(new_list) if x["name"] == name) + 1}

    # dosync() is blocking and thread-safe, perfect for Flask's multi-threaded model
    try:
        result = dosync(update_leaderboard_tx)
        return jsonify(result)
    except Exception as e:
         return jsonify({"error": str(e)}), 500

# Simulation of background tasks updating state
def background_worker():
    """Simulate random players joining the game."""
    players = ["Shadow", "Ghost", "Neo", "Trinity", "Morpheus"]
    while True:
        time.sleep(random.randint(5, 15))
        name = random.choice(players)
        score = random.randint(50, 200)
        
        def tx():
            current = leaderboard.deref()
            new_list = current + [{"name": f"{name}_{random.randint(1,99)}", "score": score}]
            new_list.sort(key=lambda x: x["score"], reverse=True)
            leaderboard.reset(new_list[:10])
        
        dosync(tx)
        print(f"[Worker] Updated leaderboard with {name}: {score}")

if __name__ == "__main__":
    # Start a background thread to show concurrent STM updates
    threading.Thread(target=background_worker, daemon=True).start()
    
    print("Starting Atomix STM Flask Demo...")
    app.run(host="0.0.0.0", port=5000, threaded=True)
