import sqlite3
import json
import argparse
import os
from datetime import datetime

def init_db(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Hands table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS hands (
        hand_id TEXT PRIMARY KEY,
        dealer_name TEXT,
        started_at TEXT
    )
    ''')

    # Players table (New mapping for id -> name)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS players (
        player_id TEXT,
        player_name TEXT,
        PRIMARY KEY (player_id, player_name)
    )
    ''')

    # Player hole cards table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS player_hand_cards (
        hand_id TEXT,
        player_id TEXT,
        hole_cards TEXT,
        PRIMARY KEY (hand_id, player_id)
    )
    ''')

    # Events table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hand_id TEXT,
        player_id TEXT,
        action TEXT,
        amount REAL,
        pot_size REAL,
        stage TEXT,     -- 'Preflop', 'Flop', 'Turn', 'River', 'Showdown'
        timestamp TEXT,
        raw_entry TEXT,
        FOREIGN KEY(hand_id) REFERENCES hands(hand_id)
    )
    ''')

    # Player Priors table (New table for long-term opponent intelligence)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS player_priors (
        player_id TEXT PRIMARY KEY,
        total_hands INTEGER,
        vpip_pct REAL,
        pfr_pct REAL,
        three_bet_pct REAL,
        wtsd_pct REAL,
        wsd_pct REAL,
        wwsf_pct REAL,
        river_bluff_freq REAL,
        avg_showdown_strength REAL,
        profile_tag TEXT
    )
    ''')

    conn.commit()
    return conn

def parse_json(json_path, db_path):
    conn = init_db(db_path)
    cursor = conn.cursor()

    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    for hand in data.get('hands', []):
        hand_id = hand['id']
        started_at = datetime.fromtimestamp(hand['startedAt'] / 1000).isoformat()
        is_cents = hand.get('cents', False)

        # map seats to player IDs and save name mappings
        seat_to_id = {}
        for p in hand.get('players', []):
            pid = p['id']
            pname = p['name']
            seat_to_id[p['seat']] = pid

            # Insert into players table mapping
            cursor.execute('INSERT OR IGNORE INTO players (player_id, player_name) VALUES (?, ?)',
                           (pid, pname))

            # Insert hole cards if available
            cards = p.get('hand')
            if cards:
                cards_str = ','.join(cards)
                cursor.execute('INSERT OR IGNORE INTO player_hand_cards (hand_id, player_id, hole_cards) VALUES (?, ?, ?)',
                               (hand_id, pid, cards_str))

        dealer_seat = hand.get('dealerSeat')
        # We don't have strictly player_name on the hands table anymore but we'll leave dealer_name as is for simplicity,
        # or we could resolve it. Let's just resolve to ID if we have it, else leave Unknown.
        # Actually dealer_name is TEXT. We can put the ID there. Let's just put the ID.
        dealer_id = seat_to_id.get(dealer_seat, "Unknown")

        cursor.execute('INSERT OR IGNORE INTO hands (hand_id, dealer_name, started_at) VALUES (?, ?, ?)',
                       (hand_id, dealer_id, started_at))

        stage = "Preflop"
        pot_size = 0.0

        for event in hand.get('events', []):
            at = event['at']
            timestamp = datetime.fromtimestamp(at / 1000).isoformat()
            payload = event.get('payload', {})
            evt_type = payload.get('type')

            seat = payload.get('seat')
            player_id = seat_to_id.get(seat) if seat else None

            action = None
            amount = 0.0

            def get_val(val):
                return float(val) / 100.0 if is_cents else float(val)

            # Map payload types to actions
            if evt_type == 3:
                action = 'post_sb'
                amount = get_val(payload.get('value', 0))
                pot_size += amount
            elif evt_type == 2:
                action = 'post_bb'
                amount = get_val(payload.get('value', 0))
                pot_size += amount
            elif evt_type in [4, 5, 6, 14]:
                action = 'post_other'
                amount = get_val(payload.get('value', 0))
                pot_size += amount
            elif evt_type == 11:
                action = 'fold'
            elif evt_type == 0:
                action = 'check'
            elif evt_type == 7:
                action = 'call'
                amount = get_val(payload.get('value', 0))
                pot_size += amount
            elif evt_type == 8:
                action = 'raise' # Covers both bet and raise
                amount = get_val(payload.get('value', 0))
                # Not perfectly accurate for pot size without full player state,
                # but we just keep it simple as in the MVP
            elif evt_type == 16:
                # Uncalled bet returned
                action = 'returned'
                amount = get_val(payload.get('value', 0))
                pot_size -= amount
            elif evt_type == 10:
                action = 'collect'
                amount = get_val(payload.get('value', payload.get('pot', 0)))
                stage = "Showdown"
            elif evt_type == 12:
                action = 'show'
                stage = "Showdown"
            elif evt_type == 9:
                # Board cards dealt
                turn = payload.get('turn')
                if turn == 1:
                    stage = "Flop"
                    action = 'deal_flop'
                elif turn == 2:
                    stage = "Turn"
                    action = 'deal_turn'
                elif turn == 3:
                    stage = "River"
                    action = 'deal_river'
                player_id = 'Dealer'
            else:
                action = 'other'

            if action and player_id:
                # Store the raw JSON payload as string just in case
                raw_entry = json.dumps(payload)
                cursor.execute('''
                INSERT INTO events (hand_id, player_id, action, amount, pot_size, stage, timestamp, raw_entry)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (hand_id, player_id, action, amount, pot_size, stage, timestamp, raw_entry))

    conn.commit()
    conn.close()

import shutil
import glob

def process_directory(input_dir, db_path):
    # Ensure ingested directory exists
    ingested_dir = os.path.join(input_dir, '..', 'ingested')
    os.makedirs(ingested_dir, exist_ok=True)

    # Process all JSON files in the directory
    search_pattern = os.path.join(input_dir, '*.json')
    files_to_process = glob.glob(search_pattern)

    if not files_to_process:
        print(f"No JSON files found in {input_dir}")
        return False

    processed_any = False
    for json_file in files_to_process:
        print(f"Processing {json_file}...")
        try:
            parse_json(json_file, db_path)

            # Move the file on success
            filename = os.path.basename(json_file)
            dest_path = os.path.join(ingested_dir, filename)
            shutil.move(json_file, dest_path)
            print(f"Successfully processed and moved {filename} to {ingested_dir}")
            processed_any = True
        except Exception as e:
            print(f"Error processing {json_file}: {e}")

    return processed_any

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('input_path', nargs='?', default='data/to_be_ingested', help='Path to JSON file or directory containing JSON files')
    parser.add_argument('--db', default='pokernow.db', help='SQLite database path')
    args = parser.parse_args()

    # Create directories if they don't exist
    if args.input_path == 'data/to_be_ingested':
        os.makedirs('data/to_be_ingested', exist_ok=True)
        os.makedirs('data/ingested', exist_ok=True)

    if os.path.isdir(args.input_path):
        process_directory(args.input_path, args.db)
    elif os.path.isfile(args.input_path):
        parse_json(args.input_path, args.db)
        print(f"Successfully processed {args.input_path} into {args.db}")
    else:
        print(f"Path not found: {args.input_path}")
