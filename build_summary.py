import json
import firebase_admin
from firebase_admin import credentials, firestore

from b2sdk.v2 import InMemoryAccountInfo, B2Api

import subprocess

import glob
import os

from dotenv import load_dotenv
load_dotenv()

STORAGE_BUCKET = os.getenv("FIREBASE_STORAGE_BUCKET")

B2_KEY_ID = os.getenv("B2_KEY_ID")
B2_APPLICATION_KEY = os.getenv("B2_APPLICATION_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")

LOCAL_EPIC_USERNAME = "BrickBoned"

SCRIPT_PATH = "C:\\Users\\harri\\Desktop\\StatPuller-Build-Match-Summary\\"

firebase_cert = SCRIPT_PATH + "fb-key.json"
parser = SCRIPT_PATH + "parser\\RocketLeagueReplayParser.exe"
last_match_stats = SCRIPT_PATH + "last-match-stats.json"
CLIPS_PATH = SCRIPT_PATH + "clips\\"


info = InMemoryAccountInfo()
b2_api = B2Api(info)
b2_api.authorize_account("production", B2_KEY_ID, B2_APPLICATION_KEY)
b2_bucket = b2_api.get_bucket_by_name(B2_BUCKET_NAME)


cred = credentials.Certificate(firebase_cert)
firebase_admin.initialize_app(cred, {
"storageBucket": STORAGE_BUCKET
})
db = firestore.client()

def main():
    latest_replay_file = SCRIPT_PATH + "last-match-replay.replay"
    data = parse_replay_to_json(latest_replay_file)

    match_stats = set_match_stats(data)
    start_epoch = data["Properties"]["MatchStartEpoch"]

    # grab all replays from local path
    clip_files = glob.glob(os.path.join(CLIPS_PATH, "*.mp4"))
    clip_files.sort()

    # upload clips for each of the local player's goals
    local_player_goal_index = 0 # used to match local player goal to its .mp4 clip
    for goal in match_stats[start_epoch]["Goals"]:
        if goal["ScorerName"] == LOCAL_EPIC_USERNAME:
            local_clip_path = clip_files[local_player_goal_index]
            remote_clip_path = f"{start_epoch}/goal_{local_player_goal_index + 1}"
            goal["GoalClip"] = upload_clip_and_get_path(local_clip_path, remote_clip_path)
            
            os.remove(local_clip_path)

            local_player_goal_index +=1

    upload_match(match_stats, db)

def parse_replay_to_json(replay_path):
    result = subprocess.run(
        [parser, replay_path],
        capture_output=True,
        text=True,
        creationflags=subprocess.CREATE_NO_WINDOW
    )
    return json.loads(result.stdout)

def set_match_stats(match_json):

    # get last match stats from bakkesmod plugin
    with open(SCRIPT_PATH + "last-match-stats.json", "r") as f:
        data = json.load(f)
        goalsUnassigned = data["Goals"]
        playlist = data["Playlist"]
    
    start_epoch = match_json["Properties"]["MatchStartEpoch"]

    goals = assign_scorers_to_goals(goalsUnassigned, match_json)
    
    match_data = {}
    bool_forfeit = "0"
    if "bForfeit" in match_json["Properties"]:
        bool_forfeit = match_json["Properties"]["bForfeit"]
    
    if "Team0Score" in match_json["Properties"]:
        team_0_score = match_json["Properties"]["Team0Score"]
    else:
        team_0_score = 0

    if "Team1Score" in match_json["Properties"]:
        team_1_score = match_json["Properties"]["Team1Score"]
    else:
        team_1_score = 0

    date_hours = match_json["Properties"]["Date"]
    date = date_hours.split(" ")[0]
    hour = date_hours.split(" ")[1]

    match_data[start_epoch] = {
        "FormatVersion": "7.1",
        "Team0Score": team_0_score,
        "Team1Score": team_1_score,
        "StartEpoch": start_epoch,
        "StartDate": date,
        "StartTime": hour,
        "LocalMMRBefore": data["MMR_Before"],
        "LocalMMRAfter": data["MMR_After"],
        "MatchPlayerInfo": [],
        "Goals": goals,
        "Playlist": playlist,
        "bForfeit": bool_forfeit
    }

    for player in match_json["Properties"]["PlayerStats"]:
        player_info = {
            "Name": player["Name"],
            "Team": player["Team"],
            "Score": player["Score"],
            "Goals": player["Goals"],
            "Assists": player["Assists"],
            "Saves": player["Saves"],
            "Shots": player["Shots"] 
        }
        match_data[start_epoch]["MatchPlayerInfo"].append(player_info)

        if player["PlayerID"][2]["EpicAccountId"]:
            player_info["EpicAccountId"] = player["PlayerID"][2]["EpicAccountId"]
        if player["OnlineID"]:
            player_info["OnlineID"] = player["OnlineID"]

        if "Value" in player["PlayerID"][3]["Platform"]:
            player_info["Platform"] = player["PlayerID"][3]["Platform"]["Value"]


    return match_data

def assign_scorers_to_goals(all_goals, match_json):
    # copy goal scorer name from replay file
    for i, goal in enumerate(all_goals):
        goal["ScorerName"] = match_json["Properties"]["Goals"][i]["PlayerName"]
    return all_goals

def upload_clip_and_get_path(local_path, remote_path):
    file_name = f"{remote_path}.mp4"
    with open(local_path, "rb") as file:
        b2_bucket.upload_bytes(file.read(), file_name)
    return file_name

def upload_match(match_data, db):
    for match_id, match_info in match_data.items():
        date = match_info["StartDate"]

        db.collection("matches") \
            .document(str(match_id)) \
            .set(match_info)
        
        db.collection("match_dates") \
            .document(date) \
            .set({})

        db.collection("latest_stats_by_playlist") \
            .document(str(match_info["Playlist"])) \
            .set({
                "CurrentMMR": match_info["LocalMMRAfter"],
                "LastPlayedDate": match_info["StartDate"],
                "LastPlayedTime": match_info["StartTime"],
                "LatestMatchId": match_info["StartEpoch"]
            })

main()
